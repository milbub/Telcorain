import numpy as np
import matplotlib.pyplot as plt

#from sklearn.linear_model import LinearRegression
#from sklearn.metrics import mean_squared_error, r2_score
#import statsmodels.api as sm

# trsl = link['trsl']
# np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
# np.array([1, 3, 2, 5, 7, 8, 8, 9, 10, 12])
# temperature_tx = np.array(link['temperature_tx'])

class Linear_regression:

    def koeficient_a(self, link):

        #x = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        #y = np.array([1, 3, 2, 5, 7, 8, 8, 9, 10, 12])

        x = np.array([[51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  ], [51.55, 51.15, 51.15, 51.  , 51.05, 51.  , 51.05, 51.15, 51.  , 51.2 , 51.05, 51.  , 51.05, 51.1 , 51.  , 51.1 , 51.  , 51.05, 51.  , 51.  , 51.1 , 51.15, 51.05, 51.1 , 51.1 , 51.  , 51.05, 51.05, 51.1 , 51.05, 51.05, 51.05, 51.  , 51.  , 51.05, 51.  , 51.05, 51.  , 51.  , 51.2 , 51.15, 51.05, 51.15, 51.55, 51.6 , 51.  , 51.05, 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  , 51.  ]])
        y = np.array([[28.  , 27.2 , 27.35, 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.2 , 28.  , 28.  , 28.  , 28.  , 28.  , 28.  , 28.  , 28.  , 28.  , 28.  , 28.15, 28.95, 29.  , 29.  , 29.  , 29.  , 29.  ], [28.  , 28.  , 28.  , 28.  , 28.  , 28.  , 27.2 , 27.3 , 27.2 , 28.  , 28.  , 28.  , 28.  , 28.  , 27.5 , 27.3 , 27.05, 27.15, 27.1 , 27.8 , 28.  , 28.  , 27.85, 27.05, 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.  , 27.9 , 28.  , 28.  , 28.  , 27.9 , 27.9 , 27.95, 27.  , 27.05, 27.85, 28.  , 28.  , 28.  , 28.45, 28.75, 28.2 , 28.7 , 29.  , 29.  , 29.  , 29.  , 29.6 , 30.  , 30.  , 30.  , 30.  ]])
        


        #x = np.array(link['temperature_tx'])
        #y = link['trsl']
        n = np.size(x)

        x_mean = np.mean(x)
        y_mean = np.mean(y)
        x_mean, y_mean

        Sxy = np.sum(x * y) - n * x_mean * y_mean
        Sxx = np.sum(x * x) - n * x_mean * x_mean

        b1 = Sxy / Sxx
        b0 = y_mean - b1 * x_mean
        print('Koeficient (a) b1 is: ', b1)
        print('Koeficient (b) b0 is: ', b0)

        """
        plt.scatter(x, y)
        plt.xlabel('Independent variable X')
        plt.ylabel('Dependent variable y')

        y_pred = b1 * x + b0

        plt.scatter(x, y, color='red')
        plt.plot(x, y_pred, color='green')
        plt.xlabel('X')
        plt.ylabel('y')
        """

        #toto su iba docasne nazvy premennych ktore sa pouziju,
        #pri aplikacii korelacneho algoritmu
        #$stanovena_teplota$ = 20
        """
        W_korig = W_orig - koeficient_a * (teplota - $stanovena_teplota$)
        IF (teplota < $stanovena_teplota$; W_orig; W_korig)
        """
