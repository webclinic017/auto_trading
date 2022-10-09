from api.FinanceDataReaderAPI import *
from util.parse_boolean import *
import importlib
import inspect
from config.at_config import get_default_config


def run_strategies(config):
    message = '-------[전략 코드 시작!]-------'
    send_message(message, LINE_MESSAGE_TOKEN)
    # markey type 에 있는 모든 종목 이름과 코드를 크롤링 한 후, 이를 db에 'code_lists' 로 저장합니다.
    market_type_list = ['NASDAQ', 'NYSE', 'ETF/KR', 'ETF/US', 'ETF/major', 'ETF/bond']  # 'KRX'
    finance_data_reader = FinanceDataReaderClass(market_type_list)
    finance_data_reader.get_code_list_of_market_by_crawling()
    print('[아래 market 들에 있는 모든 종목을 크롤링하여 DB에 저장합니다]\n' + str(market_type_list))

    p = config
    # __dict__: 객체가 가진 여러가지 속성들을 딕셔너리 형태로 편하게 확인할 수 있다.
    config_dict = p.__dict__['_config_dict_']

    # pick out strategies to use
    dict_of_invest_ratio_among_strategies = {}
    for config_name, config_value in config_dict.items():
        if config_name.startswith('USE') and config_value is True:
            strategy_name = config_name[4:]
            dict_of_invest_ratio_among_strategies[strategy_name] = 0.

    # decide invest ratio among strategies
    num_of_strategy = len(dict_of_invest_ratio_among_strategies)
    if p.invest_amount.use_equal_ratio:
        for strategy_name in dict_of_invest_ratio_among_strategies.keys():
            dict_of_invest_ratio_among_strategies[strategy_name] = 1. / num_of_strategy
    else:
        invest_ratios = [1.]  # Change this value if you want to set invest ratio by yourself.
        assert len(invest_ratios) == num_of_strategy, '전략 갯수만큼 투자 비율을 정하세요!'
        assert sum(invest_ratios) == 1., '투자 비율의 합은 1이 되어야 합니다!'
        for idx, (strategy_name, invest_ratio) in enumerate(dict_of_invest_ratio_among_strategies.items()):
            dict_of_invest_ratio_among_strategies[strategy_name] = invest_ratios[idx]
    message = '[투자 전략 종류와 투자 비율]' + '\n' + str(dict_of_invest_ratio_among_strategies)
    send_message(message, LINE_MESSAGE_TOKEN)

    for strategy_name in dict_of_invest_ratio_among_strategies.keys():
        strategy_module = importlib.import_module(name='strategy.' + strategy_name)
        list_of_cls_tuple_in_strategy_module = inspect.getmembers(strategy_module, inspect.isclass)
        """
        list_of_cls_tuple_in_strategy_module
         [
         ('BeautifulSoup', <class 'bs4.BeautifulSoup'>), 
         ('ComprehensiveDualMmtStrategy', <class 'strategy.comprehensive_dual_mmt_strategy.ComprehensiveDualMmtStrategy'>), 
         ('FinanceDataReader', <class 'api.FinanceDataReader.FinanceDataReader'>), 
         ('datetime', <class 'datetime.datetime'>)
         ]
        """
        cls_tuple_of_strategy_module = tuple(
            filter(lambda x: x[0].endswith('Strategy'), list_of_cls_tuple_in_strategy_module))[0]
        cls_of_strategy = cls_tuple_of_strategy_module[1]
        """
        cls_tuple_of_strategy_module
        ('ComprehensiveDualMmtStrategy', <class 'strategy.comprehensive_dual_mmt_strategy.ComprehensiveDualMmtStrategy'>)
        """
        amount_of_invest = p.invest_amount.total * dict_of_invest_ratio_among_strategies[strategy_name]
        instance_of_strategy = cls_of_strategy(p, amount_of_invest, finance_data_reader)
        instance_of_strategy.serve()


if __name__ == '__main__':
    config = get_default_config()
    config.add_argument("--USE_comprehensive_dual_mmt_strategy", required=False, default=True, type=parse_boolean)
    config.check_unused_keys()
    run_strategies(config)
