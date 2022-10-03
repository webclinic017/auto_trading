import sys
"""
- QApplication
    - PyQt5 를 이용하여 API 를 제어하는 메인 루프
    - OCX 방식인 API를 사용할 수 있게 됨
"""
if len(sys.argv) == 2:
    additional_investment_amount = int(sys.argv[1])
else:
    additional_investment_amount = 0

app = QApplication(sys.argv)
from strategy.comprehensive_dual_mmt_strategy import ComprehensiveDualmmtStrategyWKiwoom

comprehensive_dual_mmt_strategy = ComprehensiveDualmmtStrategyWKiwoom()
comprehensive_dual_mmt_strategy.start()
app.exec_()
