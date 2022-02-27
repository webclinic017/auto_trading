from strategy.comprehensive_dual_momentum_strategy import *
import sys
"""
- QApplication
    - PyQt5 를 이용하여 API 를 제어하는 메인 루프
    - OCX 방식인 API를 사용할 수 있게 됨
"""
app = QApplication(sys.argv)

comprehensive_dual_momentum_strategy = ComprehensiveDualMomentumSrategy()
comprehensive_dual_momentum_strategy.start()

app.exec_()
