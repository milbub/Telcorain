import numpy as np
import pandas as pd


class Algoritm:

    def algoritm(self, count, ips, curr_link, link):

        # Definovanie premennych s ktorymi sa bude pracovat
        trsl_orig = np.array(link['trsl'])
        temperature_tx = np.array(link['temperature_tx'])
        fixed_temperature = 21

        b_poletrsl = trsl_orig[0]
        b_poletemptx = temperature_tx[0]

        a_poletrsl = trsl_orig[1]
        a_poletemptx = temperature_tx[1]

        pcctrsl_a = pd.Series(b_poletrsl).corr(pd.Series(b_poletemptx))
        print(f"Korelacia 'A(rx)_B(tx)' pre spoje {count}"
              f" IP: {ips[curr_link - 1]} %0.3f" % (pcctrsl_a))

        pcctrsl_b = pd.Series(a_poletrsl).corr(pd.Series(a_poletemptx))
        print(f"Korelacia 'B(rx)_A(tx)' pre spoje {count}"
              f" IP: {ips[curr_link]} %0.3f" % (pcctrsl_b))

        if not -0.3 <= pcctrsl_a <= 0.3 or not -0.3 <= pcctrsl_b <= 0.3:
            print(f"!!! Remove link !!! - Number: {count}"
                  f" for IP_A: {ips[curr_link - 1]}"
                  f" a IP_B: {ips[curr_link]};"
                  f" Correlation: IP_A %0.3f" % (pcctrsl_a)
                  + " a IP_B %0.3f" % (pcctrsl_b))

            koeficient_ab, bb = np.polyfit(b_poletemptx, b_poletrsl, 1)

            trsl_korig_B = trsl_orig[0] - koeficient_ab * (temperature_tx[0] - fixed_temperature)
            trsl_compensated_B = np.where(temperature_tx[0] < fixed_temperature, trsl_orig[0], trsl_korig_B)

            trsl_orig[0] = trsl_compensated_B

            koeficient_ba, bb = np.polyfit(a_poletemptx, a_poletrsl, 1)

            trsl_korig_A = trsl_orig[1] - koeficient_ba * (temperature_tx[1] - fixed_temperature)
            trsl_compensated_A = np.where(temperature_tx[1] < fixed_temperature, trsl_orig[1], trsl_korig_A)

            trsl_orig[1] = trsl_compensated_A

            # Ulozenie korigovanych dovod do link['trsl']
            link['trsl'] = (["channel_id", "time"], trsl_orig)