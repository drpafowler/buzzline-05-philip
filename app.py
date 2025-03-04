import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import time

# Function to load data
def load_data():
    conn = sqlite3.connect('data/machine.sqlite')
    query = "SELECT * FROM streamed_messages"
    df = pd.read_sql_query(query, conn)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
    conn.close()
    return df.sort_values(by='timestamp', ascending=False).head(60)

# Streamlit 
st.set_page_config(layout="wide")
st.title('Machine Data Dashboard')

# Load data
df = load_data()

# Columns
col1, col2, col3 = st.columns(3)

# Bar chart for temperature 
with col1:
    st.subheader('Temperature by Machine')
    fig, ax = plt.subplots()
    for machine_id, group in df.groupby('machine_id'):
        latest_temp = group.sort_values('timestamp', ascending=False).iloc[0]['temperature']
        color = 'green' if 50 <= latest_temp <= 60 else 'red'
        ax.bar(machine_id, latest_temp, color=color)
    ax.set_xlabel('Machine ID')
    ax.set_ylabel('Current Temperature')
    st.pyplot(fig)

# Bar Graph for RPM
with col2:
    st.subheader('RPM by Machine')
    fig, ax = plt.subplots()
    for machine_id, group in df.groupby('machine_id'):
        latest_rpm = group.sort_values('timestamp', ascending=False).iloc[0]['rpm']
        color = 'green' if latest_rpm >= 4000 else 'red'
        ax.bar(machine_id, latest_rpm, color=color)
    ax.set_xlabel('Machine ID')
    ax.set_ylabel('Current RPM')
    st.pyplot(fig)

# Bar graph for conveyor speed with color coding
with col3:
    st.subheader('Conveyor Speed by Machine')
    fig, ax = plt.subplots()
    for machine_id, group in df.groupby('machine_id'):
        latest_speed = group.sort_values('timestamp', ascending=False).iloc[0]['conveyor_speed']
        color = 'green' if 1.65 <= latest_speed <= 1.85 else 'red'
        ax.bar(machine_id, latest_speed, color=color)
    ax.set_xlabel('Machine ID')
    ax.set_ylabel('Current Conveyor Speed')
    st.pyplot(fig)

# Percent stacked bar chart
with col1:
    st.subheader('Product Quality by Machine')
    quality_counts = df.groupby(['machine_id', 'product_quality']).size().unstack(fill_value=0)
    quality_percent = quality_counts.div(quality_counts.sum(axis=1), axis=0) * 100  
    quality_percent.plot(kind='bar', stacked=True, color=['red', 'green'], ax=plt.gca())
    plt.xlabel('Machine ID')
    plt.ylabel('Percent')
    plt.ylim(0, 100)  
    plt.legend(['Bad', 'Good'])
    st.pyplot(plt.gcf())

# Pie chart 
with col2:
    st.subheader('Error Code Distribution')
    error_counts = df['error_code'].value_counts()
    error_counts = error_counts[error_counts.index.notnull()]  
    fig, ax = plt.subplots()
    ax.pie(error_counts, labels=error_counts.index, autopct='%1.1f%%', colors=plt.cm.tab20.colors)
    ax.axis('equal')  
    st.pyplot(fig)

# Bar chart 
with col3:
    st.subheader('Total Widgets Produced by Machine')
    widget_counts = df.groupby('machine_id')['widgets_produced'].sum()
    fig, ax = plt.subplots()
    widget_counts.plot(kind='bar', ax=ax)
    ax.set_xlabel('Machine ID')
    ax.set_ylabel('Total Widgets Produced')
    ax.set_aspect('auto')  
    st.pyplot(fig)

# Refresh the page every 10 seconds
time.sleep(10)
st.rerun()