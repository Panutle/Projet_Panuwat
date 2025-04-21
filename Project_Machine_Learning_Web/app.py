from flask import Flask, render_template, request
import joblib
import numpy as np

loaded_model = joblib.load('Model_Machine_Learning.h5')

happy_rate = {1:'คุณมีความสุขอยู่ในระดับน้อยที่สุด',2:'คุณมีความสุขอยู่ในระดับน้อย',3:'คุณมีความสุขอยู่ในระดับปานกลาง',4:'คุณมีความสุขอยู่ในระดับมาก',5:'คุณมีความสุขอยู่ในระดับมากที่สุด'}

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/respon', methods=['POST'])
def resp():
    try:
        list_answer = []
        for i in range(29):
            num = int(request.form[f'{i+1}'])
            list_answer.append(num)

        data = np.array([list_answer])

        res = loaded_model.predict(data)

        return render_template('index.html', result=happy_rate[int(res)])

    except ValueError:
        return render_template('index.html', result='Not Respon')

if __name__ == '__main__':
    app.run(debug=True)



