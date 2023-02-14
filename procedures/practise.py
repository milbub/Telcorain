import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

trsl_orig = np.array([-49.64, -49.63, -49.58, -49.59, -49.65, -49.33, -49.29, -49.14, -48.92, -48.77, -48.62, -48.47, -48.52, -48.52, -48.40, -48.57, -48.69, -48.60, -48.59, -48.69, -48.81, -48.97, -49.02, -49.02])
temperature_tx = np.array([35.24, 34.98, 34.73, 35.36, 37.32, 40.02, 40.60, 40.95, 43.48, 45.72, 46.95, 48.64, 50.10, 50.29, 50.42, 49.40, 50.98, 49.88, 46.98, 44.45, 43.19, 42.34, 41.67, 40.88])
fixed_temperature = 23

plt.plot(temperature_tx, trsl_orig)
plt.xlabel('Btemperature_tx')
plt.ylabel('Btrsl_orig')

plt.show()
print(trsl_orig)

pcctrsl_a = pd.Series(trsl_orig).corr(pd.Series(temperature_tx))
print(f"Korelacia stara je %0.3f" % (pcctrsl_a))
"""
plt.plot(temperature_tx[1], trsl_orig[1])
plt.xlabel('Atemperature_tx')
plt.ylabel('Atrsl_orig')

plt.show()
"""

# Calculate coefficients for trsl correction for A to B pole
b_poletrsl = trsl_orig
b_poletemptx = temperature_tx
koeficient_ab, bb = np.polyfit(b_poletemptx, b_poletrsl, 1)

print(koeficient_ab)

trsl_korig_B = trsl_orig - koeficient_ab * (temperature_tx - fixed_temperature)
trsl_compensated_B = np.where(temperature_tx < fixed_temperature, trsl_orig, trsl_korig_B)

trsl_orig = trsl_compensated_B

pcctrsl_b = pd.Series(trsl_orig).corr(pd.Series(temperature_tx))
print(f"Korelacia nova je %0.3f" % (pcctrsl_b))

plt.plot(temperature_tx, trsl_orig)
plt.xlabel('temperature_tx_B')
plt.ylabel('trsl_orig_B')

plt.show()
print(trsl_orig)
"""
# Calculate coefficients for trsl correction for B to A pole
a_poletrsl = trsl_orig[1]
a_poletemptx = temperature_tx[1]
koeficient_aa, ba = np.polyfit(a_poletemptx, a_poletrsl, 1)

trsl_korig_A = trsl_orig - koeficient_aa * (temperature_tx - fixed_temperature)
trsl_compensated_A = np.where(temperature_tx < fixed_temperature, trsl_orig, trsl_korig_A)

trsl_orig[1] = trsl_compensated_A

plt.plot(temperature_tx[1], trsl_orig[1])
plt.xlabel('temperature_tx_A')
plt.ylabel('trsl_orig_A')

plt.show()trsl_orig = np.array(link['trsl'])
        temperature_tx = np.array(link['temperature_tx'])
        fixed_temperature = 23

        print(f"Orig: {trsl_orig}")

        # Calculate coefficients for trsl correction for A to B pole
        b_poletrsl = trsl_orig[0]
        b_poletemptx = temperature_tx[0]
        koeficient_ab, bb = np.polyfit(b_poletemptx, b_poletrsl, 1)

        trsl_korig_B = b_poletrsl - koeficient_ab * (b_poletemptx - fixed_temperature)
        trsl_compensated_B = np.where(b_poletemptx < fixed_temperature, b_poletrsl, trsl_korig_B)
        
        trsl_compensated_B = []
        for i in range(len(b_poletemptx)):
            if b_poletemptx[i] < fixed_temperature:
                trsl_compensated_B.append(b_poletrsl[i])
            else:
                trsl_compensated_B.append(trsl_korig_B[i])
        

        trsl_compensated_B = np.array(trsl_compensated_B)

        print(f"Orig0: {trsl_compensated_B}")

        # Calculate coefficients for trsl correction for B to A pole
        a_poletrsl = trsl_orig[1]
        a_poletemptx = temperature_tx[1]
        koeficient_aa, ba = np.polyfit(a_poletemptx, a_poletrsl, 1)

        trsl_korig_A = a_poletrsl - koeficient_aa * (a_poletemptx - fixed_temperature)
        trsl_compensated_A = np.where(a_poletemptx < fixed_temperature, a_poletrsl, trsl_korig_A)

        trsl_orig[1] = trsl_compensated_A

        print(f"Orig1: {trsl_compensated_A}")
"""