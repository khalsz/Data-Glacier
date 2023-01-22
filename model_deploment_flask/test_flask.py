# -*- coding: utf-8 -*-
"""
Created on Sun Jan 22 17:33:21 2023

@author: khalsz
"""



from flask import Flask, request,jsonify
import numpy as np
import pandas as pd
import pickle

import os
os.chdir('C:\\Users\\khalsz\\Documents\\Leicester Uni Actvt\\glacier internship\\data')

app = Flask(__name__)
model = pickle.load(open('model_house.pkl', 'rb'))


@app.route('/', methods = ['GET', 'POST'])
def home (): 
    if(request.method == 'GET'):
        data = 'Hello World'
    return jsonify({'data': data})

@app.route('/predict/')
def predict():
    LotAre = request.args.get('Lot_Area')
    BsmtFinSF = request.args.get('Bsmt_Finishing')
    TotalBsmtS = request.args.get('Total_Bsmt')
    GrLivAre = request.args.get('Living_Area')
    BsmtUnfS = request.args.get('Bsmt_Unfiinshed')
    GarageAre = request.args.get('Garage')
    X_val = pd.DataFrame({'LotArea': LotAre, 'BsmtFinSF1': BsmtFinSF, 'BsmtUnfSF': BsmtUnfS,
                          'TotalBsmtSF': TotalBsmtS, 'GrLivArea': GrLivAre, 'GarageArea': GarageAre})
    prediction = model.predict(X_val)
    
    return jsonify( {'House Price Prediction': str(prediction)})
    

if __name__ == '__main__': 
    app.run(port=5000)
