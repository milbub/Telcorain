import numpy as np

class Linear_regression:

    def compensation(self, link):

        # Defining variables to work with
        trsl_orig = np.array(link['trsl'])
        temperature_tx = np.array(link['temperature_tx'])
        fixed_temperature = 21

        # Calculate coefficients for trsl correction for A to B pole
        b_trsl_array = trsl_orig[0]
        b_temptx_array = temperature_tx[0]
        coeficient_ab, bb = np.polyfit(b_temptx_array, b_trsl_array, 1)

        trsl_corig_b = trsl_orig[0] - coeficient_ab * (temperature_tx[0] - fixed_temperature)
        trsl_compensated_b = np.where(temperature_tx[0] < fixed_temperature, trsl_orig[0], trsl_corig_b)

        trsl_orig[0] = trsl_compensated_b

        # Calculate coefficients for trsl correction for B to A pole
        a_trsl_array = trsl_orig[1]
        a_temptx_array = temperature_tx[1]
        coeficient_ba, bb = np.polyfit(a_temptx_array, a_trsl_array, 1)

        trsl_corig_a = trsl_orig[1] - coeficient_ba * (temperature_tx[1] - fixed_temperature)
        trsl_compensated_a = np.where(temperature_tx[1] < fixed_temperature, trsl_orig[1], trsl_corig_a)

        trsl_orig[1] = trsl_compensated_a

        # Saving corrected values to link['trsl']
        link['trsl'] = (["channel_id", "time"], trsl_orig)
