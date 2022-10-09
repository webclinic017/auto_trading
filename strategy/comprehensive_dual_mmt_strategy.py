from api.FinanceDataReaderAPI import *
from strategy.base_strategy import Base
from util.parse_boolean import *
from util.notifier import *
from util.crawling import *
import traceback

# stock : SPY QQQ(만 없다) DIA IWD VTI VEA VWO
# bond: TLT / IEF / SHY / TIP / LQD / HYG / AGG / BWX / EMB
# real_asset: VNQ / DBC /IAU
universe_data_dict_lists = [{
    'code': [
        'SPY', 'QQQ', 'DIA', 'IWD', 'VTI', 'VEA', 'VWO', 'TLT', 'IEF', 'SHY', 'TIP', 'LQD', 'HYG', 'AGG', 'BWX', 'EMB',
        'IAU', 'VNQ', 'DBC'
    ],
    'name': [],
    'country': [
        'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad',
        'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad'
    ],
    'category': [
        'stock', 'stock', 'stock', 'stock', 'stock', 'stock', 'stock', 'bond', 'bond', 'bond', 'bond', 'bond', 'bond',
        'bond', 'bond', 'bond', 'real_asset', 'real_asset', 'real_asset'
    ],
    'nominal_percent': [
        0.3 / 7., 0.3 / 7., 0.3 / 7., 0.3 / 7., 0.3 / 7., 0.3 / 7., 0.3 / 7., 0.55 / 9, 0.55 / 9, 0.55 / 9, 0.55 / 9,
        0.55 / 9, 0.55 / 9, 0.55 / 9, 0.55 / 9, 0.55 / 9, 0.05, 0.05, 0.05
    ],
}, {
    'code': ['SPY', 'EFA', 'EDV', 'LTPZ', 'BLV', 'EMLC', 'IAU', 'VNQ', 'DBC'],
    'name': [],
    'country': ['abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad'],
    'category': ['stock', 'stock', 'bond', 'bond', 'bond', 'bond', 'real_asset', 'real_asset', 'real_asset'],
    'nominal_percent': [0.15, 0.15, 0.2, 0.2, 0.075, 0.075, 0.05, 0.05, 0.05],
}, {
    'code': ['SPY', 'DIA', 'IWD', 'VTI', 'VEA', 'VWO', 'EDV', 'LTPZ', 'BLV', 'EMLC', 'IAU', 'VNQ', 'DBC'],
    'name': [],
    'country': [
        'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad', 'abroad',
        'abroad', 'abroad'
    ],
    'category': [
        'stock', 'stock', 'stock', 'stock', 'stock', 'stock', 'bond', 'bond', 'bond', 'bond', 'real_asset',
        'real_asset', 'real_asset'
    ],
    'nominal_percent': [0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.2, 0.2, 0.075, 0.075, 0.05, 0.05, 0.05],
}]


def set_configs(config):
    with config.namespace('universe'):
        config.index = 0
        with config.namespace('stock'):
            config.add_argument("--num_of_max_possess", default=3, type=int)
            config.fix_ratio = 'soft'
        with config.namespace('bond'):
            config.add_argument("--num_of_max_possess", default=4, type=int)
            config.fix_ratio = 'hard'
        with config.namespace('real_asset'):
            config.add_argument("--num_of_max_possess", default=2, type=int)
            config.fix_ratio = 'soft'

    with config.namespace('price_mmt'):
        with config.namespace('stock'):
            config.abs_enabled = True
            config.rel_enabled = True
            config.add_argument("--month_thres", default=[3, 6, 9], type=list)
        with config.namespace('bond'):
            config.abs_enabled = True
            config.rel_enabled = True
            config.add_argument("--month_thres", default=[3, 4, 5, 6], type=list)
        with config.namespace('real_asset'):
            config.abs_enabled = True
            config.rel_enabled = True
            config.add_argument("--month_thres", default=[4, 5, 6], type=list)
    return config


class ComprehensiveDualMmtStrategy(Base):
    def __init__(self, config, amount_of_invest, finance_data_reader):
        super().__init__(config, amount_of_invest, finance_data_reader)
        self.p = set_configs(self.p)
        self.strategy_name = 'ComprehensiveDualMmtStrategy'

        # 초기화 함수 성공 여부 확인 변수
        self.get_universe_and_price_data(universe_data_dict=universe_data_dict_lists[self.p.universe.index])

    def serve(self):
        if self.is_init_success:
            try:
                dict_of_month_lists_for_price_mmt, possess_num_dict_wrt_group = self.make_attributes_for_CDMS()
                # universe 내 모든 종목의 현재 가격을 crawling 으로 알아냅니다. ('key' = current_price)
                dicts_for_pos_abs_mmt_wrt_group = {'stock': {}, 'bond': {}, 'real_asset': {}}
                for code in self.universe_data_dict.keys():
                    category = self.universe_data_dict[code]['category']
                    self.write_month_for_price_mmt_in_db(code, category, dict_of_month_lists_for_price_mmt)
                    self.write_fix_ratio_in_db(code, possess_num_dict_wrt_group[category]['fix_ratio'])
                    if possess_num_dict_wrt_group[category]['fix_ratio'] == 'hard':
                        self.universe_data_dict[code]['have'] = 1
                        self.universe_data_dict[code]['have_percent'] = \
                        round(self.universe_data_dict[code][
                            'nominal_percent'], 3)
                    # 절대 모멘텀
                    month_lists_for_price_mmt = dict_of_month_lists_for_price_mmt[category]
                    dicts_for_pos_abs_mmt_wrt_group = self.calculate_and_write_abs_mmt_in_db(
                        code, month_lists_for_price_mmt, dicts_for_pos_abs_mmt_wrt_group)

                # 상대 모멘텀
                self.calculate_invest_stock_and_percent_wrt_rel_price_mmt(dicts_for_pos_abs_mmt_wrt_group,
                                                                          possess_num_dict_wrt_group)
                self.write_invest_stock_and_percent_in_db()
                self.send_line_message_for_invest_amount()

            except Exception as e:
                print(traceback.format_exc())
                # LINE 메시지를 보내는 부분
                send_message(traceback.format_exc(), LINE_MESSAGE_TOKEN)
        else:
            send_message('전략 인스턴스 초기화가 잘못 되었습니다!!', LINE_MESSAGE_TOKEN)

    def make_attributes_for_CDMS(self):
        possess_num_dict_wrt_group = {
            'stock': {
                'max': self.p.universe.stock.num_of_max_possess,
                'fix_ratio': self.p.universe.stock.fix_ratio
            },
            'bond': {
                'max': self.p.universe.bond.num_of_max_possess,
                'fix_ratio': self.p.universe.bond.fix_ratio
            },
            'real_asset': {
                'max': self.p.universe.real_asset.num_of_max_possess,
                'fix_ratio': self.p.universe.real_asset.fix_ratio
            }
        }
        if self.p.price_mmt.stock.abs_enabled or self.p.price_mmt.stock.rel_enabled:
            month_thres_of_stock_price_mmt = self.p.price_mmt.stock.month_thres
        else:
            month_thres_of_stock_price_mmt = None
            assert possess_num_dict_wrt_group['stock'][
                'fix_ratio'] == 'hard', '[Stock]mmt 전략을 안쓰면, fix_ratio는 hard 이어야 합니다.'
        if self.p.price_mmt.bond.abs_enabled or self.p.price_mmt.bond.rel_enabled:
            month_thres_of_bond_price_mmt = self.p.price_mmt.bond.month_thres
        else:
            month_thres_of_bond_price_mmt = None
            assert possess_num_dict_wrt_group['bond'][
                'fix_ratio'] == 'hard', '[Bond]mmt 전략을 안쓰면, fix_ratio는 hard 이어야 합니다.'
        if self.p.price_mmt.real_asset.abs_enabled or self.p.price_mmt.real_asset.rel_enabled:
            month_thres_of_real_asset_price_mmt = self.p.price_mmt.real_asset.month_thres
        else:
            month_thres_of_real_asset_price_mmt = None
            assert possess_num_dict_wrt_group['real_asset'][
                'fix_ratio'] == 'hard', '[Real Asset]mmt 전략을 안쓰면, fix_ratio는 hard 이어야 합니다.'
        dict_of_month_lists_for_price_mmt = {
            'stock': month_thres_of_stock_price_mmt,
            'bond': month_thres_of_bond_price_mmt,
            'real_asset': month_thres_of_real_asset_price_mmt
        }

        return dict_of_month_lists_for_price_mmt, possess_num_dict_wrt_group
