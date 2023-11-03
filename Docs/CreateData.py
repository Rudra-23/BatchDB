import random
import csv

data = []

for _ in range(2000):
    name = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(5))
    age = random.randint(18, 30)
    gpa = round(random.uniform(2.0, 4.0), 2)
    data.append([name, age, gpa])

# Specify the CSV file name
file_name = "student_data.csv"

# Write the data to the CSV file
with open(file_name, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(["name", "age", "gpa"])  # Write header row
    csv_writer.writerows(data)  # Write data rows

print(f"Generated and stored {len(data)} records in '{file_name}' CSV file.")
