# pip install telegram
# pip install python-telegram-bot
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator # Так как мы пишет такси в питоне
import subprocess
import sys
try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'scikit-learn'])
finally:
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.ulmer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2023, 3, 27)
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'  # через каждые 15 мин

def select(query):
    return ph.read_clickhouse(query, connection=connection)
# коннектор для БД активностей пользователей
connection = {'host':'https://clickhouse.lab.karpov.courses', 'database':'*********', 'user':'*****', 'password':'*****'}

def check_anomaly(df, metric, a=3, n=5):
    # выбираем только период за последние пять пятнадцатиминуток проследних 30 дней
    full_df = df[df['hm'].isin(df.iloc[-5:]['hm'])]
    nmp = full_df[['ts', metric]]
    # примененим алгоритм кластеризации K-Means
    # стандартизируем данные
    sc = StandardScaler()
    x_sc = sc.fit_transform(full_df[[metric]])
    # задаём модель k_means с числом кластеров 3
    cl_metric = KMeans(n_clusters = 3)
    # прогнозируем кластеры для наблюдений (алгоритм присваивает им номера от 0 до 2)
    labels = cl_metric.fit_predict(x_sc)
    # сохраняем метки кластера в поле датасета
    nmp_copy = nmp.copy()
    nmp_copy['cluster'] = labels
    # выбираем кластер, где количество элементов меньше или равно 5
    try:
        min_clusters = nmp_copy.groupby(['cluster']).nunique().reset_index().query('ts <= 8').reset_index()
        min_cluster = min_clusters.iloc[-1]['cluster']
        # делаем выборку из таблицы nmp, тоько те строки, где кластер = min_cluster (где <= 5 элементов)
        extr = nmp_copy[nmp_copy['cluster'].isin(min_clusters['cluster'])]
    except:
        print('No min cluster')
    # выбираем только период за последние два дня
    stat_df = df[df["date"] >= datetime.today() - timedelta(days = 1)].copy()
    stat_df['q25'] = stat_df[metric].shift(1).rolling(n).quantile(0.25) # вычисляем 25 квантиль
    stat_df['q75'] = stat_df[metric].shift(1).rolling(n).quantile(0.75)
    stat_df['iqr'] = stat_df['q75'] - stat_df['q25']  # разница междк квантилями
    stat_df['up'] = stat_df['q75'] + a*stat_df['iqr']  # отступы зададим 
    stat_df['low'] = stat_df['q25'] - a*stat_df['iqr']
    stat_df['up'] = stat_df['up'].rolling(n, center= True, min_periods=1).mean()  # сглаживаем границы, после сглаживания знаяения на границах (начало и конец периода) стали Nanaми - min_periods=1, чтобы не сглаживолось на концах
    stat_df['low'] = stat_df['low'].rolling(n, center= True, min_periods=1).mean()
    if metric == 'users_feed' or 'users_mess':
        a=2
    else:
        a=3
    stat_df['st'] = np.std(stat_df[metric].shift(1).iloc[-5:])
    stat_df['roll_mean'] = stat_df[metric].shift(1).rolling(n).mean()
    stat_df['up2'] = stat_df['roll_mean'] + a * stat_df['st'] 
    stat_df['low2'] = stat_df['roll_mean'] -a * stat_df['st'] 
    
    print(stat_df.tail(6))
    try:
        if ((stat_df[metric].iloc[-1] < stat_df['low'].iloc[-1] or stat_df[metric].iloc[-1] > stat_df['up'].iloc[-1])\
            or (stat_df[metric].iloc[-1] < stat_df['low2'].iloc[-1] or stat_df[metric].iloc[-1] > stat_df['up2'].iloc[-1]))\
            and extr.iloc[-1]['ts'] == df.iloc[-1]['ts']: 
            # прверяем условие алерта
            is_alert = 1
        else:
            is_alert = 0
        return is_alert, stat_df
    except:
        print('No alerts:', metric)
        is_alert = 0
        return is_alert, stat_df
        
def run_alerts(chat=None):
    chat_id = chat or -********
    bot = telegram.Bot(token='****************************')  # получаем доступ к нашему боту через его токен
    data = select (''' SELECT 
                            toStartOfFifteenMinutes(time) as ts,
                            toDate(time) as date,
                            formatDateTime(ts, '%R') as hm,
                            count(distinct user_id) as users_feed,
                            countIf(action='view') as views,
                            countIf(action='like') as likes,
                            countIf(action='like')/countIf(action='view') as CTR
                       FROM simulator_20230220.feed_actions
                       WHERE time >= today() - 30 AND time < toStartOfFifteenMinutes(now())
                       GROUP BY ts, date, hm 
                       ORDER bY ts''')
    data2 = select (''' SELECT 
                            toStartOfFifteenMinutes(time) as ts,
                            toDate(time) as date,
                            formatDateTime(ts, '%R') as hm,
                            count(distinct user_id) as users_mess, 
                            count(user_id) as cnt_mess
                       FROM simulator_20230220.message_actions
                       WHERE time >= today() - 30 AND time < toStartOfFifteenMinutes(now())
                       GROUP BY ts, date, hm 
                       ORDER bY ts''')
    
    print(data)
    metrics_list = ['users_feed', 'views', 'likes', 'users_mess', 'cnt_mess']
    for metric in metrics_list:
        # проходимся по списку метрик
        print(metric)
        if metric == 'cnt_mess' or  metric == 'users_mess':
            df= data2[['ts', 'date', 'hm', metric]].copy()
        else:
            df= data[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)
        if is_alert == 1:
            msg = "Метрика {metric}:\n текущее значение {current_val:.2f}\nотклонение от предыдущего значения {last_val_diff:.2%}\nhttp://superset.lab.karpov.courses/r/3312".format(metric= metric,\
                                                                                                                                  current_val=df[metric].iloc[-1],\
                                                                                                                                  last_val_diff = abs(1 -(df[metric].iloc[-1] / df[metric].iloc[-2])))            
            fig, axes =  plt.subplots(2, 1, figsize=(18, 12))
            fig.suptitle("Динамика метрики")
            dict = {(0): {'y': [metric, 'up', 'low'], 'title': metric},
                    (1): {'y': [metric, 'up2', 'low2'], 'title': metric}}
            
            for j in range(2):
                ax=sns.lineplot(ax=axes[j], data=df, x='ts', y=dict[j]['y'][0]) 
                ax=sns.lineplot(ax=axes[j], data=df, x='ts', y=dict[j]['y'][1])
                ax=sns.lineplot(ax=axes[j], data=df, x='ts', y=dict[j]['y'][2])
                axes[j].set_title(dict[(j)]['title'])
                axes[j].set(xlabel=None)
                axes[j].set(ylabel=None)
                for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х,
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
            
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'my_report_bot2.png' 
            plt.close()
            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)           
            
            print(msg)

with DAG('ulmer_alert', default_args=default_args, schedule_interval=schedule_interval) as dag:    
    t2 = PythonOperator(task_id='run_alerts', # Название таска
                        python_callable=run_alerts, # Название функции
                        dag=dag) # Параметры DAG
    
    # Python-операторы
    t2


