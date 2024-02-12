import cv2
import face_recognition
import pandas as pd
from datetime import datetime

current_datetime = datetime.now()

# Extract and print date, time, and day
date = current_datetime.date()
day = current_datetime.strftime("%A")

# Kindly use the path where the images of the students are at
path = "C://Users//Venu Pulagam//Desktop//train//"

# Add the roll number of a student as the key and the path of the image of the student as the value in the "people" dictionary
people = {
    'CB.EN.U4AIE21044': path + 'venu.png',
    'CB.EN.U4AIE21074': path + 'manoj.png',
    'CB.EN.U4AIE21073': path + 'lakshman.png'
}

# Load and encode template images outside the loop
known_face_encodings = []
known_face_names = []

for roll_number, filename in people.items():
    image = face_recognition.load_image_file(filename)
    encoding = face_recognition.face_encodings(image)[0]
    known_face_encodings.append(encoding)
    known_face_names.append(roll_number)


# Start camera feed
cap = cv2.VideoCapture(0)

import pandas as pd

# Replace 'your_file.csv' with the actual file path
file_path = "C://ProgramData//MySQL//MySQL Server 8.0//Uploads//Students.csv"

# Read the CSV file into a DataFrame
df = pd.read_csv(file_path, header=0)

df = df.drop("Name", axis=1)

df['SLOT1'] = ''
df['SLOT2'] = ''
df['SLOT3'] = ''

while True:
    # Read frame from camera
    ret, frame = cap.read()

    # Find all face locations and face encodings in the frame
    face_locations = face_recognition.face_locations(frame)
    face_encodings = face_recognition.face_encodings(frame, face_locations)

    # Loop over faces
    for (top, right, bottom, left), face_encoding in zip(face_locations, face_encodings):
        # Compare the face with known faces
        matches = face_recognition.compare_faces(known_face_encodings, face_encoding)

        roll_number = "Unknown"

        # If a match is found, use the roll_number of the first matching person
        if True in matches:
            first_match_index = matches.index(True)
            roll_number = known_face_names[first_match_index]

            if cv2.waitKey(1) == 13:  # 13 corresponds to the Enter key
                s1s = "08:00"
                s1e = "10:00"
                s2s = "10:20"
                s2e = "12:20"
                s3s = "14:00"
                s3e = "16:00"
                current_time = datetime.now().time()
                
                if df[df['Roll No'] == roll_number].empty:
                # If not, add a new row
                    df = pd.concat([df, pd.DataFrame({'Roll No': [roll_number], 'SLOT1': [''], 'SLOT2': [''], 'SLOT3': ['']})], ignore_index=True)

                # Update the first available slot for the given student
                if datetime.strptime(s1s, '%H:%M').time() <= current_time <= datetime.strptime(s1e, '%H:%M').time() and (str(df.loc[df['Roll No'] == roll_number, 'SLOT1'].values[0]) == '' or pd.isnull(df.loc[df['STUDENT_ID'] == roll_number, 'SLOT1'].values[0])):
                    df.loc[df['Roll No'] == roll_number, 'SLOT1'] = 1
                    print("Attendance marked for " + roll_number + "for Slot 1 !" )
                elif datetime.strptime(s2s, '%H:%M').time() <= current_time <= datetime.strptime(s2e, '%H:%M').time() and (str(df.loc[df['Roll No'] == roll_number, 'SLOT2'].values[0]) == '' or pd.isnull(df.loc[df['STUDENT_ID'] == roll_number, 'SLOT2'].values[0])):
                    df.loc[df['Roll No'] == roll_number, 'SLOT2'] = 1
                    print("Attendance marked for " + roll_number + "for Slot 2 !" )
                elif datetime.strptime(s3s, '%H:%M').time() <= current_time <= datetime.strptime(s3e, '%H:%M').time() and (str(df.loc[df['Roll No'] == roll_number, 'SLOT3'].values[0]) == '' or pd.isnull(df.loc[df['STUDENT_ID'] == roll_number, 'SLOT3'].values[0])):
                    df.loc[df['Roll No'] == roll_number, 'SLOT3'] = 1
                    print("Attendance marked for " + roll_number + "for Slot 3 !" )


        # Draw rectangle around the face
        cv2.rectangle(frame, (left, top), (right, bottom), (0, 255, 0), 2)

        # Draw label around the face if a match is found
        text_position = (left, top - 10)
        cv2.putText(frame, roll_number, text_position, cv2.FONT_HERSHEY_DUPLEX, 0.7, (0, 255, 0), 2)

    # Display the resulting frame
    cv2.imshow('Video', frame)

    # Exit the loop if the 'esc' key is pressed
    if cv2.waitKey(1) == 27:
        break

filepath = "C://Users//Venu Pulagam//Desktop//" + str(date) + "_" + str(day) + ".csv"
df.replace('', 0, inplace=True)
df.to_csv(filepath, index=False)

# Release the camera and close all windows
cap.release()
cv2.destroyAllWindows()
