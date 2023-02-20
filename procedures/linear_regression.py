import numpy as np
import pandas as pd


class Linear_regression:

    def compensation(self, link):

        # Definovanie premennych s ktorymi sa bude pracovat
        trsl_orig = np.array(link['trsl'])
        temperature_tx = np.array(link['temperature_tx'])
        fixed_temperature = 21

        # Calculate coefficients for trsl correction for A to B pole
        b_poletrsl = trsl_orig[0]
        b_poletemptx = temperature_tx[0]
        pcctrsl_a = pd.Series(b_poletrsl).corr(pd.Series(b_poletemptx))
        print(f"Korelacia stara A je %0.3f" % (pcctrsl_a))
        koeficient_ab, bb = np.polyfit(b_poletemptx, b_poletrsl, 1)

        trsl_korig_B = trsl_orig[0] - koeficient_ab * (temperature_tx[0] - fixed_temperature)
        trsl_compensated_B = np.where(temperature_tx[0] < fixed_temperature, trsl_orig[0], trsl_korig_B)

        trsl_orig[0] = trsl_compensated_B

        # Calculate coefficients for trsl correction for B to A pole
        a_poletrsl = trsl_orig[1]
        a_poletemptx = temperature_tx[1]
        koeficient_ba, bb = np.polyfit(a_poletemptx, a_poletrsl, 1)

        trsl_korig_A = trsl_orig[1] - koeficient_ba * (temperature_tx[1] - fixed_temperature)
        trsl_compensated_A = np.where(temperature_tx[1] < fixed_temperature, trsl_orig[1], trsl_korig_A)

        trsl_orig[1] = trsl_compensated_A

        # Ulozenie korigovanych dovod do link['trsl']
        link['trsl'] = (["channel_id", "time"], trsl_orig)
