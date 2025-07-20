import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import uuid
import xlsxwriter
from datetime import datetime, timedelta
import tempfile
import os
import time
import random
from gspread.exceptions import APIError
import re

# Cache Google Sheets connection
@st.cache_resource
def connect_to_gsheets():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open("TimetableData")
        return sheet
    except FileNotFoundError:
        st.error("credentials.json not found. Please place it in the project directory.")
        return None
    except Exception as e:
        st.error(f"Failed to connect to Google Sheets: {e}")
        return None

# Initialize worksheets with required headers
@st.cache_resource
def initialize_worksheets(_sheet):
    if not _sheet:
        return
    required_headers = {
        "Faculty": ["School ID", "School Name", "Department ID", "Department Name", "Faculty ID", "Faculty Name"],
        "Rooms": ["Department ID", "Room Name"],
        "Subjects": ["Subject Name", "Subject ID"],
        "Timetables": ["Timetable ID", "Date", "Department ID", "Room Name", "Faculty ID", "Subject ID", "Start Time", "End Time"]
    }
    
    try:
        existing_sheets = {ws.title: ws for ws in _sheet.worksheets()}
        for sheet_name, headers in required_headers.items():
            if sheet_name not in existing_sheets:
                _sheet.add_worksheet(title=sheet_name, rows=100, cols=20)
                existing_sheets[sheet_name] = _sheet.worksheet(sheet_name)
                existing_sheets[sheet_name].append_row(headers)
                st.info(f"Created new {sheet_name} worksheet with headers: {headers}")
            else:
                ws = existing_sheets[sheet_name]
                first_row = ws.row_values(1) if ws.row_count > 0 else []
                if not first_row or first_row != headers:
                    ws.clear()
                    ws.append_row(headers)
                    st.info(f"Reset {sheet_name} worksheet headers to: {headers}")
    except APIError as e:
        if e.response.status_code == 429:
            st.error("Google Sheets API quota exceeded. Waiting 60 seconds before retrying...")
            time.sleep(60)
            try:
                existing_sheets = {ws.title: ws for ws in _sheet.worksheets()}
                for sheet_name, headers in required_headers.items():
                    if sheet_name not in existing_sheets:
                        _sheet.add_worksheet(title=sheet_name, rows=100, cols=20)
                        existing_sheets[sheet_name] = _sheet.worksheet(sheet_name)
                        existing_sheets[sheet_name].append_row(headers)
                        st.info(f"Created new {sheet_name} worksheet with headers: {headers}")
                    else:
                        ws = existing_sheets[sheet_name]
                        first_row = ws.row_values(1) if ws.row_count > 0 else []
                        if not first_row or first_row != headers:
                            ws.clear()
                            ws.append_row(headers)
                            st.info(f"Reset {sheet_name} worksheet headers to: {headers}")
            except APIError as e2:
                st.error(f"Error initializing worksheets after retry: {e2}")
        else:
            st.error(f"Error initializing worksheets: {e}")

# Validate a timetable row
def validate_timetable_row(row, assign_missing_id=False):
    try:
        required_fields = ["Department ID", "Subject ID", "Start Time", "End Time"]
        missing_fields = [col for col in required_fields if not row.get(col)]
        if missing_fields:
            return False, f"Missing required fields: {missing_fields}"
        
        time_pattern = r"^(Monday|Tuesday|Wednesday|Thursday|Friday) \d{2}:\d{2}$"
        if not (re.match(time_pattern, row["Start Time"]) and re.match(time_pattern, row["End Time"])):
            return False, "Invalid time format"
        
        start_day, start_time = row["Start Time"].split()
        end_day, end_time = row["End Time"].split()
        if start_day != end_day:
            return False, "Start and end times must be on the same day"
        
        start_hour = int(start_time.split(":")[0])
        end_hour = int(end_time.split(":")[0])
        valid_times = ["10:00", "11:00", "12:00", "14:00", "15:00", "16:00", "17:00"]
        tenure = end_hour - start_hour
        if start_time not in valid_times or tenure <= 0 or end_hour > 18 or tenure > 3:
            return False, "Invalid or out-of-range time"
        
        if assign_missing_id and not row.get("Timetable ID"):
            row["Timetable ID"] = str(uuid.uuid4())
            st.warning(f"Assigned new Timetable ID {row['Timetable ID']} to row with Subject ID {row['Subject ID']}")
        
        return True, ""
    except Exception as e:
        return False, f"Validation error: {e}"

# Clean invalid rows from Timetables worksheet
def clean_timetables_worksheet(sheet, assign_missing_ids=False):
    if not sheet:
        return 0, []
    try:
        worksheet = sheet.worksheet("Timetables")
        data = worksheet.get_all_records()
        if not data:
            return 0, []
        
        valid_rows = [list(data[0].keys())]
        invalid_count = 0
        invalid_rows = []
        for row in data:
            is_valid, reason = validate_timetable_row(row, assign_missing_ids)
            if is_valid:
                valid_rows.append([row.get(col, "") for col in valid_rows[0]])
            else:
                invalid_count += 1
                invalid_rows.append(row)
                if reason != "Missing required fields: ['Department ID', 'Subject ID', 'Start Time', 'End Time']":
                    st.warning(f"Skipping invalid Timetables row: {reason} (Row data: {row})")
        
        if invalid_count > 0:
            worksheet.clear()
            worksheet.append_rows(valid_rows)
            st.success(f"Cleaned {invalid_count} invalid rows from Timetables worksheet.")
        return invalid_count, invalid_rows
    except APIError as e:
        st.error(f"Error cleaning Timetables worksheet: {e}")
        return 0, []

# Load data from Google Sheets
@st.cache_data(ttl=300)
def load_data(_sheet):
    if not _sheet:
        return None, None, None, None
    for attempt in range(2):
        try:
            faculty_df = pd.DataFrame(_sheet.worksheet("Faculty").get_all_records()).dropna(how="all")
            rooms_df = pd.DataFrame(_sheet.worksheet("Rooms").get_all_records()).dropna(how="all")
            subjects_df = pd.DataFrame(_sheet.worksheet("Subjects").get_all_records()).dropna(how="all")
            timetables_data = _sheet.worksheet("Timetables").get_all_records()
            
            def normalize_columns(df, expected_cols, sheet_name):
                if df.empty and sheet_name == "Timetables":
                    st.info(f"{sheet_name} worksheet is empty but headers are present. Proceeding with empty DataFrame.")
                    return pd.DataFrame(columns=expected_cols)
                df.columns = [col.strip().lower() for col in df.columns]
                missing_cols = [col for col in expected_cols if col.lower() not in df.columns]
                if missing_cols:
                    st.error(f"Missing columns in {sheet_name}: {missing_cols}. Available columns: {list(df.columns)}")
                    return None
                return df.rename(columns={col.lower(): col for col in expected_cols})
            
            faculty_df.name = "Faculty"
            rooms_df.name = "Rooms"
            subjects_df.name = "Subjects"
            
            faculty_df = normalize_columns(faculty_df, ["School ID", "School Name", "Department ID", "Department Name", "Faculty ID", "Faculty Name"], "Faculty")
            rooms_df = normalize_columns(rooms_df, ["Department ID", "Room Name"], "Rooms")
            subjects_df = normalize_columns(subjects_df, ["Subject Name", "Subject ID"], "Subjects")
            
            valid_timetables = []
            invalid_count = 0
            for row in timetables_data:
                is_valid, reason = validate_timetable_row(row, assign_missing_id=True)
                if is_valid:
                    valid_timetables.append(row)
                else:
                    invalid_count += 1
                    if reason != "Missing required fields: ['Department ID', 'Subject ID', 'Start Time', 'End Time']":
                        st.warning(f"Skipping invalid Timetables row: {reason} (Row data: {row})")
            
            if invalid_count > 0:
                st.warning(f"Skipped {invalid_count} invalid Timetables rows.")
            
            timetables_df = pd.DataFrame(valid_timetables)
            timetables_df.name = "Timetables"
            timetables_df = normalize_columns(timetables_df, ["Timetable ID", "Date", "Department ID", "Room Name", "Faculty ID", "Subject ID", "Start Time", "End Time"], "Timetables")
            
            if any(df is None for df in [faculty_df, rooms_df, subjects_df, timetables_df]):
                return None, None, None, None
            
            faculty_df = faculty_df.dropna(subset=["Faculty ID", "Faculty Name"])
            subjects_df = subjects_df.dropna(subset=["Subject Name", "Subject ID"])
            rooms_df = rooms_df.dropna(subset=["Department ID", "Room Name"])
            timetables_df = timetables_df.dropna(subset=["Timetable ID", "Department ID", "Subject ID", "Start Time", "End Time"])
            
            if faculty_df.empty or subjects_df.empty or rooms_df.empty:
                st.error("Required data is missing in Faculty, Subjects, or Rooms sheets.")
                return None, None, None, None
            
            return faculty_df, rooms_df, subjects_df, timetables_df
        except APIError as e:
            if e.response.status_code == 429 and attempt < 1:
                st.warning("Google Sheets API quota exceeded. Waiting 60 seconds before retrying...")
                time.sleep(60)
                continue
            st.error(f"Error loading data: {e}")
            return None, None, None, None
    return None, None, None, None

# Save timetable to Google Sheets
def save_timetable(timetable_data, department_id, sheet):
    if not sheet:
        return
    for attempt in range(3):
        try:
            worksheet = sheet.worksheet("Timetables")
            timetable_id = timetable_data["timetable_id"]
            batch_rows = []
            for slot in timetable_data["schedule"]:
                row = {
                    "Timetable ID": timetable_id,
                    "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Department ID": department_id,
                    "Room Name": slot["room"],
                    "Faculty ID": slot["faculty_id"],
                    "Subject ID": slot["subject_id"],
                    "Start Time": slot["start_time"],
                    "End Time": slot["end_time"]
                }
                is_valid, reason = validate_timetable_row(row)
                if not is_valid:
                    st.error(f"Cannot save invalid timetable row: {reason} (Row data: {row})")
                    return
                batch_rows.append([
                    row["Timetable ID"],
                    row["Date"],
                    row["Department ID"],
                    row["Room Name"],
                    row["Faculty ID"],
                    row["Subject ID"],
                    row["Start Time"],
                    row["End Time"]
                ])
            
            worksheet.append_rows(batch_rows)
            st.success("Timetable saved successfully!")
            load_data.clear()
            return
        except APIError as e:
            if e.response.status_code == 429 and attempt < 2:
                wait_time = 60 * (2 ** attempt)
                st.warning(f"Google Sheets API quota exceeded. Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                continue
            st.error(f"Error saving timetable: {e}")
            return

# Generate Excel file for timetable
def generate_excel(timetable_data, department_id):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        output_file = tmp.name
    workbook = xlsxwriter.Workbook(output_file)
    worksheet = workbook.add_worksheet(f"Timetable_{department_id}")
    
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#D3E4F7',
        'border': 1,
        'align': 'center',
        'valign': 'vcenter'
    })
    cell_format = workbook.add_format({
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'text_wrap': True
    })
    lunch_format = workbook.add_format({
        'bg_color': '#F0F0F0',
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'italic': True
    })
    
    time_slots = ["10:00-11:00", "11:00-12:00", "12:00-13:00", "Lunch (13:00-14:00)", "14:00-15:00", "15:00-16:00", "16:00-17:00", "17:00-18:00"]
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    
    worksheet.write(0, 0, "Day", header_format)
    for col, time_slot in enumerate(time_slots, start=1):
        worksheet.write(0, col, time_slot, header_format if time_slot != "Lunch (13:00-14:00)" else lunch_format)
    
    for row, day in enumerate(days, start=1):
        worksheet.write(row, 0, day, header_format)
        worksheet.write(row, 4, "Lunch", lunch_format)
        for col, time_slot in enumerate(time_slots, start=1):
            if time_slot == "Lunch (13:00-14:00)":
                continue
            start_time = time_slot.split('-')[0]
            for slot in timetable_data["schedule"]:
                if slot["day"] == day and slot["start_time"] == f"{day} {start_time}":
                    worksheet.write(row, col, f"{slot['subject_id']}\n({slot['faculty_id']}, {slot['room']})", cell_format)
    
    worksheet.set_column(0, 0, 15)
    worksheet.set_column(1, len(time_slots), 20)
    
    workbook.close()
    return output_file

# Check for 1-hour gap for faculty
def check_faculty_gap(schedule, faculty_id, day, start_time, tenure):
    if tenure >= 2:
        return True
    try:
        start_hour = int(start_time.split(" ")[1].split(":")[0])
        prev_hour = f"{day} {start_hour-1:02d}:00" if start_hour > 10 else None
        next_hour = f"{day} {start_hour+tenure:02d}:00" if start_hour + tenure < 17 else None
        
        for slot in schedule:
            if slot["faculty_id"] == faculty_id and slot["day"] == day:
                slot_start_hour = int(slot["start_time"].split(" ")[1].split(":")[0])
                slot_tenure = int(slot["end_time"].split(" ")[1].split(":")[0]) - slot_start_hour
                if prev_hour and slot["start_time"] == prev_hour:
                    return False
                if next_hour and slot["start_time"] == next_hour:
                    return False
                if slot_tenure > 1:
                    for h in range(slot_start_hour, slot_start_hour + slot_tenure):
                        if h == start_hour or h == start_hour + tenure:
                            return False
        return True
    except Exception as e:
        st.error(f"Error checking faculty gap: {e}")
        return False

# Check faculty's existing schedule for conflicts
def check_faculty_schedule_conflict(timetables_df, faculty_id, start_time, tenure):
    try:
        if timetables_df.empty:
            return True, ""
        
        start_day, start_hour = start_time.split()
        start_hour = int(start_hour.split(":")[0])
        end_hour = start_hour + tenure
        
        for _, row in timetables_df[timetables_df["Faculty ID"] == faculty_id].iterrows():
            row_start_day, row_start_time = row["Start Time"].split()
            row_end_time = row["End Time"].split()[1]
            if row_start_day == start_day:
                row_start_hour = int(row_start_time.split(":")[0])
                row_end_hour = int(row_end_time.split(":")[0])
                if (start_hour < row_end_hour and end_hour > row_start_hour):
                    return False, f"Faculty {faculty_id} already scheduled at {row['Start Time']} to {row['End Time']} for Subject {row['Subject ID']}"
        return True, ""
    except Exception as e:
        return False, f"Error checking faculty schedule: {e}"

# Auto-assign time slots with even distribution and cross-semester checks
def auto_assign_timeslots(faculty_assignments, dept_rooms, department_id, timetables_df, avoid_friday_afternoon=False, max_attempts=20):
    schedule = []
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    time_slots = ["10:00", "11:00", "12:00", "14:00", "15:00", "16:00", "17:00"]
    if avoid_friday_afternoon:
        time_slots = [t for t in time_slots if not (t >= "15:00" and "Friday" in days)]
    
    used_slots = {}  # Tracks room-day-time usage
    faculty_slots = {}  # Tracks faculty-day-time usage in current timetable
    room_usage = {room: 0 for room in dept_rooms["Room Name"].tolist()}
    day_usage = {faculty_id: {day: 0 for day in days} for faculty_id in {a["faculty_id"] for a in faculty_assignments}}
    
    total_classes = sum(assignment["num_classes"] for assignment in faculty_assignments)
    max_slots = len(days) * len(time_slots) * len(dept_rooms)
    if total_classes * max(assignment["tenure"] for assignment in faculty_assignments) > max_slots:
        st.warning("Insufficient time slots or rooms for all classes. Consider adding more rooms or reducing classes.")
    
    for assignment in faculty_assignments:
        faculty_id = assignment["faculty_id"]
        subject_id = assignment["subject_id"]
        num_classes = assignment["num_classes"]
        tenure = assignment["tenure"]
        pre_assigned_room = assignment.get("room", None)
        classes_assigned = 0
        failures = []
        
        # Select distinct days for the classes to ensure even distribution
        available_days = days.copy()
        if avoid_friday_afternoon:
            available_days = [d for d in available_days if d != "Friday" or not any(t >= "15:00" for t in time_slots)]
        if len(available_days) < num_classes:
            st.warning(f"Not enough days to distribute {num_classes} classes for {subject_id}. Some days may be reused.")
            selected_days = random.sample(available_days, len(available_days)) + random.choices(available_days, k=num_classes - len(available_days))
        else:
            selected_days = random.sample(available_days, num_classes)
        
        for class_idx in range(num_classes):
            day = selected_days[class_idx]
            for _ in range(max_attempts):
                if classes_assigned >= num_classes:
                    break
                shuffled_times = random.sample(time_slots, len(time_slots))
                
                for time in shuffled_times:
                    start_hour = int(time.split(":")[0])
                    if start_hour + tenure > 18 or (start_hour + tenure > 13 and start_hour < 13):
                        failures.append(f"Time {time} on {day} exceeds 18:00 or crosses lunch")
                        continue
                    
                    slot_key = f"{day}_{time}"
                    faculty_key = f"{faculty_id}_{slot_key}"
                    
                    if faculty_key in faculty_slots:
                        failures.append(f"Faculty {faculty_id} already scheduled at {time} on {day} in this timetable")
                        continue
                    
                    start_time = f"{day} {time}"
                    # Check cross-semester faculty conflicts
                    is_available, conflict_reason = check_faculty_schedule_conflict(timetables_df, faculty_id, start_time, tenure)
                    if not is_available:
                        failures.append(conflict_reason)
                        continue
                    
                    if tenure == 1 and not check_faculty_gap(schedule, faculty_id, day, start_time, tenure):
                        failures.append(f"Faculty {faculty_id} has no gap at {time} on {day}")
                        continue
                    
                    room = pre_assigned_room
                    if not room:
                        sorted_rooms = sorted(room_usage.keys(), key=lambda r: room_usage[r])
                        room = None
                        for r in sorted_rooms:
                            room_key = f"{r}_{slot_key}"
                            if room_key not in used_slots:
                                room = r
                                break
                    
                    if not room:
                        failures.append(f"No available rooms at {time} on {day}")
                        continue
                    
                    room_key = f"{room}_{slot_key}"
                    if room_key in used_slots:
                        failures.append(f"Room {room} booked at {time} on {day}")
                        continue
                    
                    end_time = f"{day} {(datetime.strptime(time, '%H:%M') + timedelta(hours=tenure)).strftime('%H:%M')}"
                    schedule.append({
                        "day": day,
                        "start_time": start_time,
                        "end_time": end_time,
                        "faculty_id": faculty_id,
                        "subject_id": subject_id,
                        "room": room,
                        "tenure": tenure
                    })
                    used_slots[room_key] = True
                    faculty_slots[faculty_key] = True
                    room_usage[room] += tenure
                    day_usage[faculty_id][day] += 1
                    classes_assigned += 1
                    break  # Move to next class after successful assignment
                
                if classes_assigned > class_idx:
                    break  # Move to next day/class if assigned
                else:
                    # If no time slot worked, try another day
                    available_days = [d for d in days if day_usage[faculty_id][d] == min(day_usage[faculty_id].values())]
                    if available_days:
                        day = random.choice(available_days)
                    else:
                        failures.append(f"No available days for {subject_id} class {class_idx+1}")
                        break
            
            if classes_assigned > class_idx:
                continue  # Move to next class
            else:
                failures.append(f"Failed to assign class {class_idx+1} for {subject_id} after {max_attempts} attempts")
                break
        
        if classes_assigned < num_classes:
            st.warning(f"Could not schedule {num_classes - classes_assigned} classes for {subject_id} (Faculty: {faculty_id}). Reasons: {set(failures) or ['Unknown constraints']}")
    
    return schedule

# Streamlit App
def main():
    st.set_page_config(page_title="Time Table Generator 🕒", layout="wide")
    
    if "show_splash" not in st.session_state:
        st.session_state.show_splash = True
        st.session_state.splash_start = time.time()
    if "page" not in st.session_state:
        st.session_state.page = "create"
    if "faculty_assignments" not in st.session_state:
        st.session_state.faculty_assignments = []
    if "temp_assignments" not in st.session_state:
        st.session_state.temp_assignments = []
    if "widget_counter" not in st.session_state:
        st.session_state.widget_counter = 0
    
    st.markdown("""
        <style>
        .main { background: linear-gradient(to right, #e6f0fa, #ffffff); }
        .stButton>button {
            background: linear-gradient(to right, #4facfe, #00f2fe);
            color: white;
            border-radius: 10px;
            padding: 10px 20px;
            font-size: 16px;
            border: none;
            transition: all 0.3s ease;
        }
        .stButton>button:hover {
            background: linear-gradient(to right, #00f2fe, #4facfe);
            transform: scale(1.05);
        }
        .tooltip {
            position: relative;
            display: inline-block;
        }
        .tooltip .tooltiptext {
            visibility: hidden;
            width: 120px;
            background-color: #555;
            color: #fff;
            text-align: center;
            border-radius: 6px;
            padding: 5px;
            position: absolute;
            z-index: 1;
            bottom: 125%;
            left: 50%;
            margin-left: -60px;
            opacity: 0;
            transition: opacity 0.3s;
        }
        .tooltip:hover .tooltiptext {
            visibility: visible;
            opacity: 1;
        }
        </style>
    """, unsafe_allow_html=True)
    
    if st.session_state.show_splash:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.title("Welcome to Time Table Generator 🕒")
            try:
                st.image("logo.png", caption="Timetable Generator", width=300)
            except FileNotFoundError:
                st.image("https://via.placeholder.com/300", caption="Timetable Generator", width=300)
            
            if st.button("Continue to Dashboard 🚀", key="splash_continue", help="Click to start creating timetables"):
                st.session_state.show_splash = False
                st.rerun()
        
        if time.time() - st.session_state.splash_start >= 3:
            st.session_state.show_splash = False
            st.rerun()
        return
    
    sheet = connect_to_gsheets()
    if not sheet:
        return
    
    initialize_worksheets(sheet)
    
    # Clean Timetables worksheet on startup
    with st.spinner("Cleaning invalid Timetables data..."):
        invalid_count, invalid_rows = clean_timetables_worksheet(sheet, assign_missing_ids=True)
        if invalid_count > 0 and invalid_rows:
            st.warning(f"Removed {invalid_count} invalid Timetables rows. Download invalid rows for review?")
            if st.button("Download Invalid Rows CSV", key="download_invalid_rows"):
                invalid_df = pd.DataFrame(invalid_rows)
                csv = invalid_df.to_csv(index=False)
                st.download_button(
                    label="Download Invalid Rows",
                    data=csv,
                    file_name="invalid_timetables.csv",
                    mime="text/csv",
                    key="download_invalid_csv"
                )
    
    with st.sidebar:
        st.subheader("Dashboard 📋")
        hour = datetime.now().hour
        greeting = "Good Morning" if hour < 12 else "Good Afternoon" if hour < 18 else "Good Evening"
        st.write(f"{greeting}, Timetable Creator! 🌟")
        
        st.markdown('<div class="tooltip">', unsafe_allow_html=True)
        if st.button("📅 Create Timetable", key="dash_create"):
            st.session_state.page = "create"
            st.session_state.widget_counter += 1
            st.rerun()
        st.markdown('<span class="tooltiptext">Create a new timetable</span></div>', unsafe_allow_html=True)
        
        st.markdown('<div class="tooltip">', unsafe_allow_html=True)
        if st.button("📜 Previous Timetables", key="dash_previous"):
            st.session_state.page = "previous"
            st.session_state.widget_counter += 1
            st.rerun()
        st.markdown('<span class="tooltiptext">View past timetables</span></div>', unsafe_allow_html=True)
        
        st.markdown('<div class="tooltip">', unsafe_allow_html=True)
        if st.button("👩‍🏫 Perspective View", key="dash_perspective"):
            st.session_state.page = "perspective"
            st.session_state.widget_counter += 1
            st.rerun()
        st.markdown('<span class="tooltiptext">See faculty schedules</span></div>', unsafe_allow_html=True)
    
    if st.session_state.page == "create":
        create_timetable_page(sheet)
    elif st.session_state.page == "previous":
        previous_timetables_page(sheet)
    elif st.session_state.page == "perspective":
        perspective_view_page(sheet)

# Create Timetable Page
def create_timetable_page(sheet):
    st.title("Create New Timetable 📅")
    
    with st.spinner("Loading data..."):
        faculty_df, rooms_df, subjects_df, timetables_df = load_data(sheet)
    if faculty_df is None:
        return
    
    if faculty_df.empty or rooms_df.empty or subjects_df.empty:
        st.error("Required data is missing. Please check Faculty, Rooms, and Subjects sheets.")
        return
    
    st.subheader("Basic Information ℹ️")
    school_name = st.selectbox("School Name", sorted(faculty_df["School Name"].unique()), key=f"school_{st.session_state.widget_counter}")
    department_name = st.selectbox("Department Name", sorted(faculty_df["Department Name"].unique()), key=f"dept_{st.session_state.widget_counter}")
    department_id = faculty_df[faculty_df["Department Name"] == department_name]["Department ID"].iloc[0] if not faculty_df.empty else ""
    semester = st.selectbox("Semester", [1, 2, 3, 4, 5, 6, 7, 8], key=f"sem_{st.session_state.widget_counter}")
    year = st.number_input("Year", min_value=2000, max_value=2100, value=2025, key=f"year_{st.session_state.widget_counter}")
    
    dept_faculty = faculty_df[faculty_df["Department ID"] == department_id]
    dept_rooms = rooms_df[rooms_df["Department ID"] == department_id]
    
    if dept_faculty.empty or dept_rooms.empty:
        st.error("No faculty or rooms available for the selected department.")
        return
    
    st.subheader("Assign Faculty and Subjects 👩‍🏫📚")
    st.info("Add faculty assignments below. 'Hours/Class' is the duration per class (e.g., 1 = 1 hour, 3 = 3 hours continuous).")
    
    st.markdown("**Faculty Assignments**")
    col1, col2, col3, col4, col5, col6 = st.columns([2, 2, 1, 1, 1, 1])
    with col1:
        st.markdown("**Faculty**")
    with col2:
        st.markdown("**Subject**")
    with col3:
        st.markdown("**Classes/Week**")
    with col4:
        st.markdown('<div class="tooltip">**Hours/Class**<span class="tooltiptext">Duration per class (1-3 hours)</span></div>', unsafe_allow_html=True)
    with col5:
        st.markdown("**Room**")
    with col6:
        st.markdown("**Action**")
    
    for idx, assignment in enumerate(st.session_state.temp_assignments[:]):
        with st.container():
            col1, col2, col3, col4, col5, col6 = st.columns([2, 2, 1, 1, 1, 1])
            with col1:
                faculty_id = st.selectbox(
                    "Faculty",
                    options=dept_faculty["Faculty ID"].tolist(),
                    format_func=lambda x: dept_faculty[dept_faculty["Faculty ID"] == x]["Faculty Name"].iloc[0],
                    key=f"faculty_{idx}_{st.session_state.widget_counter}",
                    index=dept_faculty["Faculty ID"].tolist().index(assignment["faculty_id"]) if assignment["faculty_id"] in dept_faculty["Faculty ID"].tolist() else 0
                )
            with col2:
                subject_id = st.selectbox(
                    "Subject",
                    options=subjects_df["Subject ID"].tolist(),
                    format_func=lambda x: subjects_df[subjects_df["Subject ID"] == x]["Subject Name"].iloc[0],
                    key=f"subject_{idx}_{st.session_state.widget_counter}",
                    index=subjects_df["Subject ID"].tolist().index(assignment["subject_id"]) if assignment["subject_id"] in subjects_df["Subject ID"].tolist() else 0
                )
            with col3:
                num_classes = st.number_input(
                    "Classes",
                    min_value=1,
                    max_value=10,
                    value=assignment["num_classes"],
                    step=1,
                    key=f"classes_{idx}_{st.session_state.widget_counter}"
                )
            with col4:
                tenure = st.number_input(
                    "Hours",
                    min_value=1,
                    max_value=3,
                    value=assignment["tenure"],
                    step=1,
                    key=f"tenure_{idx}_{st.session_state.widget_counter}"
                )
            with col5:
                room = st.selectbox(
                    "Room",
                    options=[""] + dept_rooms["Room Name"].tolist(),
                    key=f"room_{idx}_{st.session_state.widget_counter}",
                    index=dept_rooms["Room Name"].tolist().index(assignment["room"]) + 1 if assignment["room"] and assignment["room"] in dept_rooms["Room Name"].tolist() else 0
                )
            with col6:
                if st.button("🗑️", key=f"remove_{idx}_{st.session_state.widget_counter}"):
                    st.session_state.temp_assignments.pop(idx)
                    st.rerun()
            
            st.session_state.temp_assignments[idx] = {
                "faculty_id": faculty_id,
                "faculty_name": dept_faculty[dept_faculty["Faculty ID"] == faculty_id]["Faculty Name"].iloc[0],
                "subject_id": subject_id,
                "subject_name": subjects_df[subjects_df["Subject ID"] == subject_id]["Subject Name"].iloc[0],
                "num_classes": int(num_classes),
                "tenure": int(tenure),
                "room": room if room else None
            }
    
    if st.button("➕ Add Assignment", key=f"add_assignment_{st.session_state.widget_counter}"):
        st.session_state.temp_assignments.append({
            "faculty_id": dept_faculty["Faculty ID"].iloc[0],
            "faculty_name": dept_faculty["Faculty Name"].iloc[0],
            "subject_id": subjects_df["Subject ID"].iloc[0],
            "subject_name": subjects_df["Subject Name"].iloc[0],
            "num_classes": 1,
            "tenure": 1,
            "room": None
        })
        st.rerun()
    
    if st.button("✅ Confirm Assignments", key=f"confirm_assignments_{st.session_state.widget_counter}"):
        if not st.session_state.temp_assignments:
            st.error("Please add at least one assignment.")
        else:
            st.session_state.faculty_assignments = st.session_state.temp_assignments
            st.success("Assignments confirmed!")
            st.session_state.widget_counter += 1
            st.rerun()
    
    with st.form(key=f"timetable_form_{st.session_state.widget_counter}"):
        st.subheader("Assign Rooms and Time Slots 🕒")
        st.info("Manually assign time slots (and rooms if not pre-assigned) or enable auto-assignment. Lunch break is reserved from 13:00-14:00.")
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        time_slots = ["10:00", "11:00", "12:00", "14:00", "15:00", "16:00", "17:00"]
        schedule = []
        
        auto_assign = st.checkbox("Auto-assign time slots", key=f"auto_assign_{st.session_state.widget_counter}")
        avoid_friday_afternoon = st.checkbox("Avoid scheduling on Friday afternoons", key=f"avoid_friday_{st.session_state.widget_counter}") if auto_assign else False
        
        if st.session_state.faculty_assignments and not auto_assign:
            for assignment in st.session_state.faculty_assignments:
                faculty_id = assignment["faculty_id"]
                faculty_name = assignment["faculty_name"]
                subject_id = assignment["subject_id"]
                subject_name = assignment["subject_name"]
                num_classes = assignment["num_classes"]
                tenure = assignment["tenure"]
                pre_assigned_room = assignment.get("room", None)
                
                with st.expander(f"Schedule {subject_name} ({subject_id}, Faculty: {faculty_name}) - {num_classes} classes"):
                    for class_idx in range(num_classes):
                        st.markdown(f"**Class {class_idx+1}**")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            day = st.selectbox(
                                "Day",
                                options=days,
                                key=f"day_{faculty_id}_{subject_id}_{class_idx}_{st.session_state.widget_counter}"
                            )
                        with col2:
                            time = st.selectbox(
                                "Time",
                                options=time_slots,
                                key=f"time_{faculty_id}_{subject_id}_{class_idx}_{st.session_state.widget_counter}"
                            )
                        with col3:
                            if pre_assigned_room:
                                st.write(f"Room: {pre_assigned_room} (Pre-assigned)")
                                room = pre_assigned_room
                            else:
                                room = st.selectbox(
                                    "Room",
                                    options=dept_rooms["Room Name"].tolist(),
                                    key=f"room_{faculty_id}_{subject_id}_{class_idx}_{st.session_state.widget_counter}"
                                )
                        
                        start_time = f"{day} {time}"
                        start_hour = int(time.split(":")[0])
                        if start_hour + tenure > 18 or (start_hour + tenure > 13 and start_hour < 13):
                            st.error(f"Class at {start_time} exceeds 18:00 or crosses lunch (13:00-14:00).")
                            schedule = []
                            break
                        
                        end_time = f"{day} {(datetime.strptime(time, '%H:%M') + timedelta(hours=tenure)).strftime('%H:%M')}"
                        
                        if any(slot["start_time"] == start_time and slot["room"] == room for slot in schedule):
                            st.error(f"Room {room} is booked at {start_time}. Please choose another room or time.")
                            schedule = []
                            break
                        
                        if tenure == 1 and not check_faculty_gap(schedule, faculty_id, day, start_time, tenure):
                            st.error(f"Faculty {faculty_id} has a class in an adjacent time slot on {day}.")
                            schedule = []
                            break
                        
                        schedule.append({
                            "day": day,
                            "start_time": start_time,
                            "end_time": end_time,
                            "faculty_id": faculty_id,
                            "subject_id": subject_id,
                            "room": room,
                            "tenure": tenure
                        })
        
        submitted = st.form_submit_button("🚀 Generate Timetable")
    
    if submitted:
        if not st.session_state.faculty_assignments:
            st.error("Please confirm at least one faculty assignment.")
            return
        
        if auto_assign:
            with st.spinner("Generating timetable automatically..."):
                try:
                    schedule = auto_assign_timeslots(st.session_state.faculty_assignments, dept_rooms, department_id, timetables_df, avoid_friday_afternoon)
                except Exception as e:
                    st.error(f"Error in auto-assignment: {e}")
                    return
        
        has_errors = False
        if not auto_assign:
            room_conflicts = {}
            faculty_conflicts = {}
            faculty_gap_issues = {}
            
            for i, slot_i in enumerate(schedule):
                for j, slot_j in enumerate(schedule):
                    if i != j and slot_i["start_time"] == slot_j["start_time"]:
                        if slot_i["room"] == slot_j["room"]:
                            room_conflicts[f"{slot_i['room']} at {slot_i['start_time']}"] = True
                        if slot_i["faculty_id"] == slot_j["faculty_id"]:
                            faculty_conflicts[f"{slot_i['faculty_id']} at {slot_i['start_time']}"] = True
            
            for i, slot_i in enumerate(schedule):
                for j, slot_j in enumerate(schedule):
                    if i != j and slot_i["faculty_id"] == slot_j["faculty_id"] and slot_i["day"] == slot_j["day"] and slot_i["tenure"] == 1 and slot_j["tenure"] == 1:
                        time_i = int(slot_i["start_time"].split(" ")[1].split(":")[0])
                        time_j = int(slot_j["start_time"].split(" ")[1].split(":")[0])
                        if abs(time_i - time_j) == 1:
                            faculty_gap_issues[f"{slot_i['faculty_id']} on {slot_i['day']} at {time_i}:00 and {time_j}:00"] = True
            
            if room_conflicts:
                for conflict in room_conflicts:
                    st.error(f"Room conflict: {conflict}")
                has_errors = True
            
            if faculty_conflicts:
                for conflict in faculty_conflicts:
                    st.error(f"Faculty conflict: {conflict}")
                has_errors = True
            
            if faculty_gap_issues:
                for issue in faculty_gap_issues:
                    st.error(f"Faculty has no gap between classes: {issue}")
                has_errors = True
        
        if not has_errors:
            total_classes = sum(assignment["num_classes"] for assignment in st.session_state.faculty_assignments)
            if len(schedule) < total_classes:
                st.warning(f"Scheduled {len(schedule)}/{total_classes} classes. Consider adding more rooms or adjusting assignments.")
            
            timetable_data = {
                "timetable_id": str(uuid.uuid4()),
                "school_name": school_name,
                "department_name": department_name,
                "semester": semester,
                "year": year,
                "schedule": schedule
            }
            save_timetable(timetable_data, department_id, sheet)
            
            st.subheader("Timetable Preview 📊")
            time_slots_display = ["10:00-11:00", "11:00-12:00", "12:00-13:00", "Lunch (13:00-14:00)", "14:00-15:00", "15:00-16:00", "16:00-17:00", "17:00-18:00"]
            days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
            table_data = [["" for _ in range(len(time_slots_display))] for _ in range(len(days))]
            
            time_slot_map = {
                "10:00": 0,
                "11:00": 1,
                "12:00": 2,
                "14:00": 4,
                "15:00": 5,
                "16:00": 6,
                "17:00": 7
            }
            
            for i, day in enumerate(days):
                table_data[i][3] = "Lunch"
                for slot in schedule:
                    try:
                        start_time = slot["start_time"].split(" ")[1]
                        tenure = slot["tenure"]
                        start_hour = int(start_time.split(":")[0])
                        if start_time not in time_slot_map:
                            st.warning(f"Skipping invalid start time {start_time} for {slot['subject_id']}.")
                            continue
                        time_idx = time_slot_map[start_time]
                        if slot["day"] == day and table_data[i][time_idx] == "":
                            table_data[i][time_idx] = f"{slot['subject_id']} ({slot['faculty_id']}, {slot['room']})"
                            for h in range(1, tenure):
                                next_hour = start_hour + h
                                if next_hour in [10, 11, 12, 14, 15, 16, 17]:
                                    next_idx = time_slot_map[f"{next_hour}:00"]
                                    if table_data[i][next_idx] == "":
                                        table_data[i][next_idx] = "↳ (cont.)"
                    except (KeyError, IndexError, ValueError) as e:
                        st.warning(f"Skipping invalid slot for {slot['subject_id']}: {e}")
            
            df = pd.DataFrame(table_data, index=days, columns=time_slots_display)
            st.table(df)
            
            excel_file = generate_excel(timetable_data, department_id)
            with open(excel_file, "rb") as f:
                st.download_button(
                    label="Download Timetable 📥",
                    data=f,
                    file_name=f"timetable_{timetable_data['timetable_id']}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"download_timetable_{st.session_state.widget_counter}"
                )
            os.unlink(excel_file)

# Previous Timetables Page
def previous_timetables_page(sheet):
    st.title("Previous Timetables 📜")
    
    with st.spinner("Loading timetables..."):
        _, _, _, timetables_df = load_data(sheet)
    
    if timetables_df is None or timetables_df.empty:
        st.info("No valid timetables found.")
        return
    
    grouped = timetables_df.groupby(["Timetable ID", "Department ID"])
    time_slot_map = {
        "10:00": 0,
        "11:00": 1,
        "12:00": 2,
        "14:00": 4,
        "15:00": 5,
        "16:00": 6,
        "17:00": 7
    }
    
    for i, ((timetable_id, dept_id), group) in enumerate(grouped):
        if not timetable_id or not dept_id:
            st.warning(f"Skipping timetable with missing ID or Department ID (Timetable ID: {timetable_id or 'None'}, Dept ID: {dept_id or 'None'})")
            continue
        date = group["Date"].iloc[0] if not group["Date"].empty and pd.notna(group["Date"].iloc[0]) else "Unknown"
        with st.expander(f"Department ID: {dept_id}, Timetable ID: {timetable_id}, Generated: {date}"):
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("View Timetable", key=f"view_timetable_{i}_{st.session_state.widget_counter}"):
                    time_slots = ["10:00-11:00", "11:00-12:00", "12:00-13:00", "Lunch (13:00-14:00)", "14:00-15:00", "15:00-16:00", "16:00-17:00", "17:00-18:00"]
                    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
                    table_data = [["" for _ in range(len(time_slots))] for _ in range(len(days))]
                    for j, day in enumerate(days):
                        table_data[j][3] = "Lunch"
                    for _, row in group.iterrows():
                        try:
                            start_day, time = row["Start Time"].split()
                            end_day, end_time = row["End Time"].split()
                            if start_day != end_day:
                                st.warning(f"Skipping invalid slot for {row['Subject ID'] or 'Unknown'}: Start and end days differ (Timetable ID: {timetable_id})")
                                continue
                            tenure = int(end_time.split(":")[0]) - int(time.split(":")[0])
                            start_hour = int(time.split(":")[0])
                            if time not in time_slot_map:
                                st.warning(f"Skipping invalid start time {time} for {row['Subject ID'] or 'Unknown'} (Timetable ID: {timetable_id})")
                                continue
                            time_idx = time_slot_map[time]
                            day_idx = days.index(start_day)
                            table_data[day_idx][time_idx] = f"{row['Subject ID']} ({row['Faculty ID']}, {row['Room Name']})"
                            for h in range(1, tenure):
                                next_hour = start_hour + h
                                if next_hour in [10, 11, 12, 14, 15, 16, 17]:
                                    next_idx = time_slot_map[f"{next_hour}:00"]
                                    if table_data[day_idx][next_idx] == "":
                                        table_data[day_idx][next_idx] = "↳ (cont.)"
                        except (ValueError, IndexError, KeyError, AttributeError) as e:
                            st.warning(f"Skipping invalid slot for {row.get('Subject ID', 'Unknown')} in Timetable {timetable_id}: {e} (Row data: {row})")
                    
                    st.dataframe(pd.DataFrame(table_data, index=days, columns=time_slots))
            
            with col2:
                if st.button("Download Excel", key=f"download_excel_{i}_{st.session_state.widget_counter}"):
                    schedule = [
                        {
                            "day": row["Start Time"].split()[0],
                            "start_time": row["Start Time"],
                            "end_time": row["End Time"],
                            "faculty_id": row["Faculty ID"],
                            "subject_id": row["Subject ID"],
                            "room": row["Room Name"],
                            "tenure": int(row["End Time"].split()[1].split(":")[0]) - int(row["Start Time"].split()[1].split(":")[0])
                        }
                        for _, row in group.iterrows()
                        if re.match(r"^(Monday|Tuesday|Wednesday|Thursday|Friday) \d{2}:\d{2}$", row["Start Time"])
                        and re.match(r"^(Monday|Tuesday|Wednesday|Thursday|Friday) \d{2}:\d{2}$", row["End Time"])
                        and row["Start Time"].split()[0] == row["End Time"].split()[0]
                    ]
                    
                    excel_file = generate_excel({"schedule": schedule}, dept_id)
                    with open(excel_file, "rb") as f:
                        st.download_button(
                            label="Download Excel File",
                            data=f,
                            file_name=f"timetable_{timetable_id}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"download_excel_file_{i}_{st.session_state.widget_counter}"
                        )
                    os.unlink(excel_file)

# Perspective View Page
def perspective_view_page(sheet):
    st.title("Faculty Perspective View 👩‍🏫")
    
    with st.spinner("Loading data..."):
        faculty_df, _, _, timetables_df = load_data(sheet)
    
    if faculty_df is None or faculty_df.empty:
        st.error("No faculty data available.")
        return
    
    with st.form(key=f"perspective_form_{st.session_state.widget_counter}"):
        faculty_id = st.selectbox(
            "Select Faculty",
            sorted(faculty_df["Faculty ID"].tolist()),
            format_func=lambda x: faculty_df[faculty_df["Faculty ID"] == x]["Faculty Name"].iloc[0],
            key=f"faculty_select_{st.session_state.widget_counter}"
        )
        if st.form_submit_button("View Schedule"):
            if timetables_df is None or timetables_df.empty:
                st.info("No timetables available.")
                return
            
            faculty_schedule = timetables_df[timetables_df["Faculty ID"] == faculty_id][
                ["Timetable ID", "Date", "Department ID", "Room Name", "Subject ID", "Start Time", "End Time"]
            ]
            
            if faculty_schedule.empty:
                st.info("No classes assigned to this faculty.")
            else:
                st.write(f"Total Classes: {len(faculty_schedule)}")
                st.dataframe(faculty_schedule)

if __name__ == "__main__":
    main()