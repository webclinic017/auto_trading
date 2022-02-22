from api.Kiwoom import *
import sys

"""
- QApplication
    - PyQt5 를 이용하여 API 를 제어하는 메인 루프
    - OCX 방식인 API를 사용할 수 있게 됨
"""
app = QApplication(sys.argv)
kiwoom = Kiwoom()


# kospi code list/code_name 를 받겠다.
get_kospi_code_list = False
if get_kospi_code_list:
    kopsi_code_list = kiwoom.get_code_list_by_market("0") # list # "10": KOSDAQ
    print("kopsi_code_list:", kopsi_code_list)
    for code in kopsi_code_list:
        code_name = kiwoom.get_master_code_name(code)
        print('kospi_code:', code, ' / kospi_code_name:', code_name)
#
#
# 삼성전자(005930)의 일봉 정보를 출력하겠다.
get_samsung_price_data = False
if get_samsung_price_data:
    df = kiwoom.get_price_data(code="005930", date="20220211")
    print(df)

# 예수금을 확인하겠다.
get_deposit = False
if get_deposit:
    deposit = kiwoom.get_deposit()

# 오늘 주문 했던 거래들 조희하기 (다음 날 장 거래 전까지 유효)
get_order = True
orders = kiwoom.get_order()
print('today`s orders:', order)


# 주문 접수 및 채결 확인
    # send_buy_order: rqname
    # '1001' : 화면 번호
    # 1: 신규 매수 주문 / 2: 신규 매도 주문 / 3: 매수 취소 / 4: 매도 취소 / 5: 매수 정정 / 6: 매도 정정
    # '007000': 매수할 종목 코드
    # 1: 주문 수량
    # 35000: 주문 가격
    # '00' : 지정가 주문 방식
send_order = True
if send_order:
    order_result = kiwoom.send_order('send_buy_order', '1001', 1, '007700', 1, 35000, '00')
    print('order_result:', order_result)


# 잔고 얻어 오기 (구매한 종목들 확인)
get_balance = True
if get_balance:
    position = kiwoom.get_balance()
    print('내가 구입한 종목들:', position)

# 실시간 체결 정보 얻어 오기
    # 특징
        # 실시간으로 바뀌기 때문에, 요청 > 응답 대기 > 응답 방식 (TR 방식) 외 다른 방법이 필요함
            # 기존: SetInputValue -> CommRqData -> OnReceiveTrData -> OnReceiveRealData
        # SetREalReg: TR 서비스 조회 요청 없이 실시간 시세 등록이 가능
            # 새로운 방법: SetRealReg -> OnReceiveRealData
    # 결과물
    # 삼성전자 / 채결 시간 / 종가 / 고가 / 시가 / 저가 / 최우선 매도 호가 매수 호가 / 당일 누적 거래량

    # 한번 등록하면 계쏙 데이터를 얻어옴 -> print 를 지우는게 좋다.
set_real_reg = True
if set_real_reg:
    # 장 시작 시간 얻어오기
    # "1000": 화면 번호 / "": 특정 종목에 대한 정보를 얻는 것이 아니므로 / get_fid("장운영구분") / 0: 최초등록, 1: 최초등록이 아님
    kiwoom.set_real_reg("1000", "", get_fid("장운영구분"), "0")


    # 현재는 fid 에 채결시간만 넣어도, 모든 정보들을 다 알 수 있다.
    fids = get_fid("체결시간")
    codes = '005930;007700;000660;' # 확인할 종목 코드들
    kiwoom.set_real_reg("1000", codes, fids, "0")

app.exec_()