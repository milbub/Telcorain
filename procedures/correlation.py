import numpy as np
import pandas as pd


class Correlation:

    def pearson_correlation(self, count, ips, curr_link,
                            link_todelete, link):

        trsl = link['trsl']
        temperature_tx = np.array(link['temperature_tx'])

        b_poletrsl = trsl[0]
        b_poletemptx = temperature_tx[0]

        a_poletrsl = trsl[1]
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
            link_todelete.append(link)
