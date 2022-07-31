import pandas as pd

df_agent =pd.read_csv('/Users/rajeshchoudhury/PycharmProjects/mydb/Agent_Login_Report.xlsx - Agent Login Report.csv')
df_performance = pd.read_csv('/Users/rajeshchoudhury/PycharmProjects/mydb/AgentPerformance.xlsx - Agent Performance Report.csv')

df_performance['coverted_date']= pd.to_datetime(df_performance['Date'])

df_performance['days_in_week_year']= df_performance['coverted_date'].dt.weekofyear
#q.1
df_resp = df_performance.groupby(['Agent Name','days_in_week_year'])['Average Rating'].mean()

print(df_resp)
#q.2
df_resp = df_agent.groupby('Agent')['Date'].count()
print(df_resp)
#q.3
df_resp = df_performance.groupby('Agent Name')['Total Chats'].sum()
print(df_resp)
#q.4
df_resp = df_performance.groupby('Agent Name')['Total Feedback'].sum()
print(df_resp)
#q.5
df_resp = df_performance[(df_performance['Average Rating'] > 3.5) & (df_performance['Average Rating'] <4)]['Agent Name']
print(df_resp)
#q.6
df_resp = df_performance[df_performance['Average Rating']<3.5]['Agent Name']
print(df_resp)
#q.7
df_resp = df_performance[df_performance['Average Rating']>4.5]['Agent Name']
print(df_resp)


#q.8
df_resp = [df_performance.groupby('Agent Name')['Total Feedback'].agg('mean')>4.5]['Agent Name']

print(df_resp)

df = [df_performance.groupby('Agent Name')['Total Feedback'].mean()>4.5]['Agent Name']
#q.9
df_resp = df_performance.groupby(['Agent Name','days_in_week_year'])['Average Response Time'].mean()
print(df_resp)
#q.10
df_resp = df_performance.groupby(['Agent Name','days_in_week'])['Average Resolution Time'].mean()
print(df_resp)

#q.11
df_resp= set(df_performance['Agent Name'])
print(df_resp)

#q.12
