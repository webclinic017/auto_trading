from util.parse_boolean import parse_boolean
from datetime import datetime
import numpy as np
import nestargs
from nestargs.parser import NestedNamespace
import pickle
import yaml
import json
from threading import Lock


class AttrDict(dict):

    def __init__(self):
        super(AttrDict, self).__setattr__('_allow_new_entry_', True)

    def __getattr__(self, name):
        if name.startswith('_') and name.endswith('_'):
            return super(AttrDict, self).__getattr__(name)
        return self.__getitem__(name)

    def __setattr__(self, k, v):
        if k in self.keys():
            dest = self.__getitem__(k)
            dest_ad = type(dest) == AttrDict
            src_ad = type(v) == AttrDict
            if dest_ad and src_ad:
                dest.merge(v)
            elif not (dest_ad or src_ad):
                self.__setitem__(k, v)
            elif dest_ad and not src_ad:
                raise ValueError('[ATConfig] AttrDict %s cannot be overwritten by other type %s' % (k, str(type(v))))
            elif not dest_ad and src_ad:
                raise ValueError('[ATConfig] %s (Type %s) cannot be overwritten by an AttrDict' %
                                 (k, str(type(dest))))
        else:
            if not self._allow_new_entry_:
                raise ValueError('[ATConfig] Cannot add a new entry \'%s\' into a locked AttrDict' % k)
            self.__setitem__(k, v)

    def __str__(self):
        return json.dumps(self, indent=4, sort_keys=True)

    def merge(self, target):
        for k, v in target.items():
            self.__setattr__(k, v)

    def set_new_entry_allowed(self, value):
        super(AttrDict, self).__setattr__('_allow_new_entry_', value)


class NamespaceContext:

    def __init__(self, target, name, on_cmd=False):
        self.target = target
        self.name = name
        self.on_cmd = on_cmd
        self.dict_backup = None
        self.namespace_backup = ''
        self.on_cmd_backup = None

    def __enter__(self):
        target_temp = self.target._config_dict_
        name_spl = self.name.split('.')
        for name in name_spl:
            if name not in target_temp.keys():
                target_temp.__setattr__(name, AttrDict())
            target_temp = target_temp.__getattr__(name)

        self.dict_backup = self.target._config_dict_
        self.namespace_backup = self.target._namespace_
        self.on_cmd_backup = self.target._namespace_on_cmd_
        self.target._config_dict_ = target_temp
        if self.target._namespace_ != '':
            self.target._namespace_ = self.target._namespace_ + self.target._delimiter_ + self.name
        else:
            self.target._namespace_ = self.name
        self.target._namespace_on_cmd_ = self.on_cmd

    def __exit__(self, *args):
        self.target._config_dict_ = self.dict_backup
        self.target._namespace_ = self.namespace_backup
        self.target._namespace_on_cmd_ = self.on_cmd_backup
        self.dict_backup = self.on_cmd_backup = None
        self.namespace_backup = ''


def recursive_entry_lock(target, value):
    if type(target) == AttrDict:
        target.set_new_entry_allowed(value)
        for k, v in target.items():
            recursive_entry_lock(v, value)
    elif type(target) == ATConfig:
        recursive_entry_lock(target._config_dict_, value)
    return True


class ATConfigLockContext:

    def __init__(self, target):
        self.target = target

    def __enter__(self):
        recursive_entry_lock(self.target, False)
        self.target._allow_new_entry_ = False

    def __exit__(self, *args):
        recursive_entry_lock(self.target, True)
        self.target._allow_new_entry_ = True


'''
                                np.generic,np.number,np.integer,np.signedinteger,np.byte,np.short,np.intc,np.int_,
                                np.longlong,np.unsignedinteger,np.ubyte,np.ushort,np.uintc,np.uint,np.ulonglong,
                                np.inexact,np.floating,np.half,np.single,np.double,np.longdouble,np.complexfloating,
                                np.csingle,np.cdouble,np.clongdouble
'''

ATConfig_supported_types_ = [
    str,
    bool,
    int,
    float,
    complex,
    list,
    tuple,
    set,
    dict,
    np.ndarray,
    AttrDict,
    type(None),
    np.int16,
    np.float16,
    np.int8,
    np.uint64,
    np.void,
    np.uint32,
    np.complex128,
    np.unicode_,
    np.uint32,
    np.complex64,
    np.string_,
    np.uint16,
    np.timedelta64,
    np.bool_,
    np.uint8,
    np.datetime64,
    np.object_,
    np.int64,
    np.int32,
    np.float64,
    np.int32,
    np.float32,
]


class ATConfig(NestedNamespace):

    def __init__(self, conflict_handler='warning'):
        super().__setattr__('_lock_', Lock())
        self._conflict_handler_ = conflict_handler
        assert (self._conflict_handler_ in ['error', 'resolve', 'warning', 'ask'])
        self._delimiter_ = '.'
        self._fake_parser_ = nestargs.NestedArgumentParser(conflict_handler='resolve',
                                                           add_help=True,
                                                           allow_abbrev=False)
        self._namespace_ = ''
        self._namespace_on_cmd_ = False
        self._used_keys_ = []
        self._unused_keys_ = []
        self._config_dict_ = AttrDict()
        self._allow_new_entry_ = True

    def namespace(self, name, on_cmd=False):
        with self._lock_:
            if on_cmd:
                print('[ATConfig] Warning! on_cmd is deprecated and will be deleted after test')
            return NamespaceContext\
                (self, name, on_cmd)

    def new_entry_lock(self):
        return ATConfigLockContext(self)

    def add_argument(self, *args, **kwargs):
        if args is None or args[0][0] not in self._fake_parser_.prefix_chars:
            raise ValueError('[ATConfig] add_argument() must have positional argument with prefix character!')
        if len(args) > 1:
            raise ValueError('[ATConfig] Only one positional argument is allow for config')
        if 'dest' in kwargs:
            raise ValueError('[ATConfig] cannot use dest keyword argument in configs!')
        delimiter = args[0][0]
        split = args[0].split(args[0][0])
        num_delimiters = len(split) - 1
        for i in range(len(split) - 1):
            split[i] += delimiter
        if split[-1].startswith('_'):
            raise ValueError('[ATConfig] the name of the argument should not start from "_"')

        ns = '' if self._namespace_ == '' else self._namespace_ + '.'
        full_argument_name = delimiter * num_delimiters + ns + split[-1]
        last_argument_name = delimiter * num_delimiters + split[-1].split('.')[-1]
        if full_argument_name == last_argument_name:
            args = (full_argument_name, )
        else:
            args = (last_argument_name, full_argument_name)

        kwargs['dest'] = split[-1]

        # if 'dest' in kwargs:
        #     print(kwargs['dest'])
        #     kwargs['dest'] = self._namespace_ + self._delimiter_ + kwargs['dest']

        self._fake_parser_.add_argument(*args, **kwargs)
        temp_parser = nestargs.NestedArgumentParser(add_help=False, allow_abbrev=False)
        temp_parser.add_argument(*args, **kwargs)
        parsed_args, unused_keys = temp_parser.parse_known_args()
        parsed_dict = ATConfig.get_attrdict_from_nested_namespace(parsed_args)

        for a in args:
            if a in self._used_keys_:
                if self._conflict_handler_ == 'error':
                    raise ValueError('[ATConfig] ERROR : CLI argument %s has been overwritten!!' % a)
                elif self._conflict_handler_ == 'warning':
                    print('[ATConfig] WARNING : CLI argument %s has been overwritten!!' % a)
                elif self._conflict_handler_ == 'ask':
                    answer = input('[ATConfig] CLI argument %s has been overwritten! Do you want to continue?(y/n)' %
                                   a)
                    if answer == 'y':
                        pass
                    elif answer == 'n':
                        raise ValueError('[ATConfig] ERROR : CLI argument %s has been overwritten!!' % a)
                    else:
                        raise ValueError('[ATConfig] Invalid Answer!')
            else:
                self._used_keys_.append(a)

        self._config_dict_.merge(parsed_dict)

        return None

    @staticmethod
    def check_type(target):
        if type(target) not in ATConfig_supported_types_:
            raise TypeError('[ATConfig] type %s is not supported in config' % str(type(target)))
        elif type(target) == NestedNamespace:
            target = ATConfig.get_attrdict_from_nested_namespace(target)
        elif type(target) == dict:
            target = ATConfig.dict_to_attrdict(target)
        elif type(target) == np.ndarray:
            print('[ATConfig] WARNING : numpy array type is not supported in config -> converted to list')
            target = target.tolist()
        elif type(target) in [tuple, set]:
            print('[ATConfig] WARNING : %s type is not supported in config -> converted to list' %
                  (str(type(target))))
            target = list(target)

        return target

    def check_unused_keys(self):
        parsed_args, unused_keys = self._fake_parser_.parse_known_args()
        if len(unused_keys) > 0:
            raise ValueError('[ATConfig] There are un-used arguments : ', unused_keys)

    def __getattr__(self, name):
        try:
            return self._config_dict_.__getitem__(name)
        except KeyError as err:
            msg = ('[ATConfig] Cannot find the key %s under the namespace %s' % (name, self._namespace_), )
            for i in range(1, len(err.args)):
                msg = msg + (err.args[i], )
            err.args = msg
            raise

    def __setattr__(self, name, value):
        if name.startswith('_') and name.endswith('_'):
            super().__setattr__(name, value)
        else:
            value = ATConfig.check_type(value)
            self._config_dict_.__setattr__(name, value)

    @staticmethod
    def get_attrdict_from_nested_namespace(target):
        result = AttrDict()
        for k, v in target.__dict__.items():
            if k.startswith('_') and k.endswith('_') and type(target) == ATConfig: continue
            if type(v) == NestedNamespace:
                v = ATConfig.get_attrdict_from_nested_namespace(v)
            elif type(v) == dict:
                v = ATConfig.dict_to_attrdict(v)
            else:
                v = ATConfig.check_type(v)
            result[k] = v
        return result

    @staticmethod
    def dict_to_attrdict(target):
        result = AttrDict()
        for k, v in target.items():
            if type(v) == dict:
                v = ATConfig.dict_to_attrdict(v)
            elif type(v) == NestedNamespace:
                v = ATConfig.get_attrdict_from_nested_namespace(v)
            else:
                v = ATConfig.check_type(v)
            result[k] = v
        return result

    def import_from(self, target):
        content = AttrDict()
        if type(target) == str:
            # file name
            ext = target.split('.')[-1]
            if ext == 'pkl':
                f = open(target, 'rb')
                content = pickle.load(f)
                f.close()
            elif ext == 'json':
                f = open(target, 'r')
                content = json.load(f)
                f.close()
            elif ext == 'yaml':
                f = open(target, 'r')
                content = yaml.load(f)
                f.close()
            else:
                raise ValueError('[ATConfig] Cannot import a file with extention %s. (We support pkl, json, yaml)' %
                                 ext)

        elif type('target') == dict:
            content = target

        else:
            raise ValueError('[ATConfig] Import only supports string or dict type target')

        content = ATConfig.dict_to_attrdict(content)

        for k, v in content.items():
            self._config_dict_[k] = v

    def export_to(self, target=None):
        if target is not None and type(target) != str:
            ValueError('[ATConfig] Invalid target for export (None and string are supported)')

        if target is not None:
            ext = target.split('.')[-1]
            if ext == 'pkl':
                f = open(target, 'wb')
                pickle.dump(self._config_dict_, f)
                f.close()
            elif ext == 'json':
                f = open(target, 'w')
                json.dump(self._config_dict_, f, indent=4, sort_keys=True)
                f.close()
            elif ext == 'yaml':
                f = open(target, 'w')
                yaml.dump(self._config_dict_, f)
                f.close()
            else:
                raise ValueError(
                    '[ATConfig] Cannot export to a file with extention %s. (We support pkl, json, yaml)' % ext)

    def __str__(self):
        return str(self._config_dict_)


def get_default_config():
    config = ATConfig()
    with config.namespace('invest_amount'):
        config.add_argument("--total", default=0., type=float)
        config.add_argument("--use_equal_ratio", required=False, default=True,
                            type=parse_boolean)

    # config.acc_actor_step = 0


    return config
