from flask import Flask, render_template
import mysql.connector

app = Flask(__name__)

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="iot_project"
)

@app.route("/")
def dashboard():
    cursor = db.cursor()
    cursor.execute("SELECT * FROM sensor_data ORDER BY timestamp DESC")
    data = cursor.fetchall()
    return render_template("index.html", data=data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)