import numpy as np
import pandas as pd


def compensation(count, ips, curr_link, link, spin_correlation):

    # spin_correlation is the set value of the correlation from which the compensation algorithm is performed

    # Defining variables to work with
    trsl_orig = np.array(link['trsl'])
    temperature_tx = np.array(link['temperature_tx'])
    fixed_temperature = 21

    b_trsl_array = trsl_orig[0]
    b_temptx_array = temperature_tx[0]

    a_trsl_array = trsl_orig[1]
    a_temptx_array = temperature_tx[1]

    # Calculation of the Pearson correlation index for side A
    pcctrsl_a = pd.Series(b_trsl_array).corr(pd.Series(b_temptx_array))
    # print(f"Correlation 'A(rx)_B(tx)' for link {count}"
    # f" IP: {ips[curr_link - 1]} %0.3f" % (pcctrsl_a))

    # Calculation of the Pearson correlation index for side B
    pcctrsl_b = pd.Series(a_trsl_array).corr(pd.Series(a_temptx_array))
    # print(f"Correlation 'B(rx)_A(tx)' for link {count}"
    # f" IP: {ips[curr_link]} %0.3f" % (pcctrsl_b))

    if not (np.isnan(pcctrsl_a) or np.isnan(pcctrsl_b)):
        if ((pcctrsl_a >= spin_correlation) or (pcctrsl_a <= -spin_correlation)) \
                or ((pcctrsl_b >= spin_correlation) or (pcctrsl_b <= -spin_correlation)):
            print(f"!!! Compensated link !!! - Number: {count}"
                  f" for IP_A: {ips[curr_link - 1]}"
                  f" a IP_B: {ips[curr_link]};"
                  f" Correlation: IP_A %0.3f" % pcctrsl_a
                  + " a IP_B %0.3f" % pcctrsl_b)
            coeficient_ab, bb = np.polyfit(b_temptx_array, b_trsl_array, 1)

            trsl_corig_b = trsl_orig[0] - coeficient_ab * (temperature_tx[0] - fixed_temperature)
            trsl_compensated_b = np.where(temperature_tx[0] < fixed_temperature, trsl_orig[0], trsl_corig_b)

            trsl_orig[0] = trsl_compensated_b

            coeficient_ba, bb = np.polyfit(a_temptx_array, a_trsl_array, 1)

            trsl_corig_a = trsl_orig[1] - coeficient_ba * (temperature_tx[1] - fixed_temperature)
            trsl_compensated_a = np.where(temperature_tx[1] < fixed_temperature, trsl_orig[1], trsl_corig_a)

            trsl_orig[1] = trsl_compensated_a

            # Saving corrected values to link['trsl']
            link['trsl'] = (["channel_id", "time"], trsl_orig)
    else:
        pass
