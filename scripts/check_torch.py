import traceback
try:
    import torch
    print('torch', torch.__version__)
except Exception:
    traceback.print_exc()
