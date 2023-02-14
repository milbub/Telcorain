import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

trsl_orig = np.array([[1, 3, 4, 6, 3, 4.2, 5, 6, 4, 3, 6, 4], [6, 6, 4, 6, 3, 4, 5, 6, 4, 6, 6, 4]])
temperature_tx = np.array([20, 24, 26, 27, 22, 22, 26, 29, 24, 21, 29, 25])
fixed_temperature = 23

print(trsl_orig)
pcctrsl_a = pd.Series(trsl_orig[0]).corr(pd.Series(temperature_tx))
print(f"Korelacia stara A je %0.3f" % (pcctrsl_a))
pcctrsl_b = pd.Series(trsl_orig[1]).corr(pd.Series(temperature_tx))
print(f"Korelacia stara B je %0.3f" % (pcctrsl_b))


# Calculate coefficients for trsl correction for A to B pole
b_poletrsl = trsl_orig[0]
b_poletemptx = temperature_tx
koeficient_ab, bb = np.polyfit(b_poletrsl, b_poletemptx, 1)

trsl_korig_B = trsl_orig[0] - koeficient_ab * (temperature_tx - fixed_temperature)
trsl_compensated_B = np.where(temperature_tx < fixed_temperature, trsl_orig[0], trsl_korig_B)

trsl_orig[0] = trsl_compensated_B

# Calculate coefficients for trsl correction for B to A pole
a_poletrsl = trsl_orig[1]
a_poletemptx = temperature_tx
koeficient_ba, bb = np.polyfit(a_poletrsl, a_poletemptx, 1)

trsl_korig_A = trsl_orig[1] - koeficient_ba * (temperature_tx - fixed_temperature)
trsl_compensated_A = np.where(temperature_tx < fixed_temperature, trsl_orig[1], trsl_korig_A)

trsl_orig[1] = trsl_compensated_A


print(trsl_orig)
pcctrsl_a = pd.Series(trsl_orig[0]).corr(pd.Series(temperature_tx))
print(f"Korelacia nova A je %0.3f" % (pcctrsl_a))
pcctrsl_b = pd.Series(trsl_orig[1]).corr(pd.Series(temperature_tx))
print(f"Korelacia nova B je %0.3f" % (pcctrsl_b))

