from api.FinanceDataReaderAPI import *
from util.parse_boolean import *
from util.db_helper import *
from util.time_helper import *
from util.notifier import *
from util.crawling import *
import traceback
import numpy as np


class Base:
    def __init__(self, config, amount_of_invest, finance_data_reader):
        self.p = config
        self.amount_of_invest = amount_of_invest
        # 초기화 함수 성공 여부 확인 변수
        self.finance_data_reader = finance_data_reader
        self.strategy_name = None
        self.is_init_success = False

    def get_universe_and_price_data(self, universe_data_dict=None):
        """
        - Objectives
            - check_and_get_universe
                - 전략 관련 db 가 없으면, universe.py 에서 가져온 universe 를 db에 'universe' 테이블로 만듭니다.
                - 전략 관련 db 의 'universe' 테이블에서, universe를 가져옵니다.

            - get_price_data_by_crawling_and_make_db
                - universe의 가격 정보를 crawling으로 가져오고, 이를 db로 만듭니다.
        """
        try:
            # 유니버스 조회, 없으면 생성
            self.universe_data_dict = self.check_and_get_universe(universe_data_dict)

            # 가격 정보를 조회, 필요하면 생성
            if self.p.crawl_price_data:
                self.get_price_data_by_crawling_and_make_db()
            else:
                self.load_price_data_in_db()

            for code in self.universe_data_dict.keys():
                self.write_current_price_in_db(code)
            self.is_init_success = True

        except Exception as e:
            print(traceback.format_exc())
            # LINE 메시지를 보내는 부분
            send_message(traceback.format_exc(), LINE_MESSAGE_TOKEN)

    def check_and_get_universe(self, universe_data_dict):
        # Base db 내에 'universe' 테이블이 없으면
        now = datetime.now().strftime("%Y%m%d")
        # FinanceDataReader 로 부터 code(Symbol) 찾기
        self.finance_data_reader.make_universe_table_at_db(self.strategy_name, universe_data_dict, now)

        # universe 테이블에서 모든 것을 select 하자.
        universe_data_dict = self.get_universe_from_db(db_name=self.strategy_name)
        return universe_data_dict

    def get_universe_from_db(self, db_name=None):
        universe_data_dict = {}
        sql = "select * from 'universe'"
        cur = execute_sql(db_name, sql)
        db_universe_dict = cur.fetchall()  # fetchall: select 문의 결과 객체를 이용하여, 조회 결과 확인 가능
        for _, item in enumerate(db_universe_dict):
            _, code, code_name, country, category, percent, created_at, fix_ratio, abs_mmt, rel_mmt, have, have_percent, mmt_month, data_len = item
            universe_data_dict[code] = {
                'code_name': code_name,
                'country': country,
                'category': category,
                'nominal_percent': round(percent, 3),
                'created_at': created_at,
                'fix_ratio': fix_ratio,
                'abs_mmt': abs_mmt,
                'rel_mmt': rel_mmt,
                'have': have,
                'have_percent': round(have_percent, 3),
                'mmt_month': mmt_month,
                'data_len': data_len
            }
        # send_message('[UNIVERSE LIST] \n\n\n' + str(self.universe_data_dict), LINE_MESSAGE_TOKEN)
        return universe_data_dict

    def get_price_data_by_crawling_and_make_db(self):
        """일봉 데이터가 존재하는지 확인하고 없다면 생성하는 함수"""
        for idx, code in enumerate(self.universe_data_dict.keys()):
            print("[get_price_data_by_crawling_and_make_db]({}/{}) {}".format(idx + 1, len(self.universe_data_dict),
                                                                              code))
            strategy_db_name = self.strategy_name
            self.universe_data_dict = self.finance_data_reader.get_price_data_by_crawling_and_make_db(
                strategy_db_name, self.universe_data_dict, code)

    def load_price_data_in_db(self):
        for code in self.universe_data_dict.keys():
            sql = "select * from '{}'".format(code)
            data = execute_sql_as_dataframe(self.strategy_name, sql)
            self.universe_data_dict[code]['price_df'] = data

    def calculate_invest_percent_wrt_group(self):
        dict_for_nominal_invest_percent_wrt_group = {'stock': 0., 'bond': 0., 'real_asset': 0.}
        for code in self.universe_data_dict.keys():
            dict_for_nominal_invest_percent_wrt_group[self.universe_data_dict[code]
                                                      ['category']] += self.universe_data_dict[code]['nominal_percent']
        return dict_for_nominal_invest_percent_wrt_group

    def write_month_for_price_mmt_in_db(self, code, category, dict_of_month_lists_for_price_mmt):
        sql = "update universe set mmt_month=:mmt_month where code=:code"
        execute_sql(self.strategy_name, sql, {
            "mmt_month": str(dict_of_month_lists_for_price_mmt[category]),
            "code": code
        })

    def write_fix_ratio_in_db(self, code, fix_ratio):
        sql = "update universe set fix_ratio=:fix_ratio where code=:code"
        execute_sql(self.strategy_name, sql, {"fix_ratio": str(fix_ratio), "code": code})

    def write_current_price_in_db(self, code):
        price_df = self.universe_data_dict[code]['price_df']['Close'].copy()
        if self.universe_data_dict[code]['country'] == 'korea':
            # TODO: we do not have to divide method for crawling btw korea and abroad
            self.universe_data_dict[code]['current_price'] = float(get_realtime_price_korea(code))
        else:
            self.universe_data_dict[code]['current_price'] = float(price_df[0])

    def calculate_and_write_abs_mmt_in_db(self, code, month_lists_for_price_mmt, dicts_for_pos_abs_mmt_wrt_group):
        # 절대 모멘텀
        if month_lists_for_price_mmt is None:
            abs_mmt = None
        else:
            abs_mmt = 0.
            price_df = self.universe_data_dict[code]['price_df']['Close'].copy()

            if isinstance(month_lists_for_price_mmt, int):
                month_lists_for_price_mmt = [month_lists_for_price_mmt]
            assert isinstance(month_lists_for_price_mmt, list), 'month_lists_for_price_mmt 는 리스트 이어야 합니다.'
            for price_mmt_month in month_lists_for_price_mmt:
                price_mmt_day = int(price_mmt_month * (365 / 12))
                past_average_price = price_df[price_mmt_day - 15:price_mmt_day + 14].mean()
                abs_mmt += (self.universe_data_dict[code]['current_price'] / past_average_price)

            abs_mmt *= (1 / len(month_lists_for_price_mmt))
            abs_mmt = np.round(abs_mmt, 2)

        self.universe_data_dict[code]['abs_mmt'] = abs_mmt
        sql = "update universe set abs_mmt=:abs_mmt where code=:code"
        execute_sql(self.strategy_name, sql, {"abs_mmt": self.universe_data_dict[code]['abs_mmt'], "code": code})
        if abs_mmt is not None:
            if abs_mmt >= 1.:
                dicts_for_pos_abs_mmt_wrt_group[self.universe_data_dict[code]
                                                ['category']][code] = self.universe_data_dict[code]['abs_mmt']
        return dicts_for_pos_abs_mmt_wrt_group

    def calculate_invest_stock_and_percent_wrt_rel_price_mmt(self, dicts_for_pos_abs_mmt_wrt_group,
                                                             possess_num_dict_wrt_group):
        dict_for_nominal_invest_percent_wrt_group = self.calculate_invest_percent_wrt_group()
        dicts_for_invest_code_list_wrt_group = {'stock': [], 'bond': [], 'real_asset': []}
        for category_name in dicts_for_pos_abs_mmt_wrt_group.keys():
            dict_of_pos_abs_mmt_in_group = dicts_for_pos_abs_mmt_wrt_group[category_name]
            # dicts를 abs mmt 기준으로 정렬하겠다.
            dict_of_pos_abs_mmt_in_group = dict(sorted(dict_of_pos_abs_mmt_in_group.items(), key=lambda x: x[1]))
            # 최대 소유 갯수 만큼, 상대 모멘텀 순위를 매겨 최종적으로 투자할 종목들을 추려낸다.
            dicts_for_invest_code_list_wrt_group[category_name] = list(
                dict_of_pos_abs_mmt_in_group.keys())[:possess_num_dict_wrt_group[category_name]['max']]
            for rel_mmt_rank, code_for_invest in enumerate(dicts_for_invest_code_list_wrt_group[category_name]):
                self.universe_data_dict[code_for_invest]['rel_mmt'] = rel_mmt_rank
                if possess_num_dict_wrt_group[category_name]['fix_ratio'] == 'mixed':
                    self.universe_data_dict[code_for_invest]['have'] = 1
                    self.universe_data_dict[code_for_invest]['have_percent'] = round(
                        self.universe_data_dict[code_for_invest]['nominal_percent'], 3)
                elif possess_num_dict_wrt_group[category_name]['fix_ratio'] == 'soft':
                    self.universe_data_dict[code_for_invest]['have'] = 1
                    unit_have_percent_wrt_group = dict_for_nominal_invest_percent_wrt_group[
                        category_name] / possess_num_dict_wrt_group[category_name]['max']
                    self.universe_data_dict[code_for_invest]['have_percent'] = round(unit_have_percent_wrt_group, 3)

    def write_invest_stock_and_percent_in_db(self):
        for code in self.universe_data_dict.keys():
            # update have
            sql = "update universe set have=:have where code=:code"
            execute_sql(self.strategy_name, sql, {"have": self.universe_data_dict[code]['have'], "code": code})
            # update have_percent
            sql = "update universe set have_percent=:have_percent where code=:code"
            execute_sql(self.strategy_name, sql, {
                "have_percent": round(self.universe_data_dict[code]['have_percent'], 3),
                "code": code
            })
            # update rel_mmt
            sql = "update universe set rel_mmt=:rel_mmt where code=:code"
            execute_sql(self.strategy_name, sql, {"rel_mmt": self.universe_data_dict[code]['rel_mmt'], "code": code})

    def send_line_message_for_invest_amount(self):
        for category in ['stock', 'bond', 'real_asset']:
            for idx, code in enumerate(self.universe_data_dict.keys()):
                if self.universe_data_dict[code]['category'] == category:
                    universe_data_dict_for_log = dict(
                        (i, self.universe_data_dict[code][i]) for i in self.universe_data_dict[code] if i != 'price_df')
                    send_message(
                        '[UNIVERSE 상태:{}]\n\n\n'.format(category + '/' + code) + str(universe_data_dict_for_log),
                        LINE_MESSAGE_TOKEN)
                    indiv_amount_of_invest = np.round(self.amount_of_invest *
                                                      universe_data_dict_for_log['have_percent'])
                    if indiv_amount_of_invest > 0:
                        send_message(
                            '[투자해야할 금액!!!!!!! 상태:{} --> 구매금액/총 투자 금액]\n\n\n'.format(code) +
                            str(indiv_amount_of_invest) + '/' + str(self.amount_of_invest), LINE_MESSAGE_TOKEN)
