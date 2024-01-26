from flask import Flask, request, jsonify
import csv
from datetime import datetime, timedelta

app = Flask(__name__)

@app.route('/get_data', methods=['GET'])
def get_data():
    requested_date_str = request.args.get('date')

    if requested_date_str is None:
        return jsonify({"error": "Veuillez spécifier une date"}), 400

    try:
        requested_date = datetime.strptime(requested_date_str, "%Y-%m-%d-%H:%M")
    except ValueError:
        return jsonify({"error": "Format de date invalide. Utilisez le format YYYY/MM/DD-HH:MM"}), 400

    with open('../eco2mix-regional-cons-def.csv', 'r') as file:
        csv_reader = csv.DictReader(file, delimiter=';')
        for entry in csv_reader:
            entry_datetime_str = f"{entry['Date']}-{entry['Heure']}"
            entry_datetime = datetime.strptime(entry_datetime_str, '%Y-%m-%d-%H:%M')

            if entry_datetime == requested_date:
                requested_data = entry

    if not requested_data:
        return jsonify({"error": "Aucune donnée disponible pour la date spécifiée"}), 404

    return jsonify(requested_data)

if __name__ == '__main__':
    app.run(debug=True)