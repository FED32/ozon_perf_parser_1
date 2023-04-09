import requests
import json
from datetime import datetime
from datetime import timedelta
from datetime import date
import time
import os
import pandas as pd
import numpy as np
import glob
import zipfile
import psycopg2
from sqlalchemy import create_engine
# from contextlib import closing


class OzonPerformance:
    def __init__(self, client_id, client_secret,
                 account_id=None,
                 day_lim=70,
                 camp_lim=8):
        self.account_id = account_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.methods = {'statistics': 'https://performance.ozon.ru:443/api/client/statistics',
                        'phrases': 'https://performance.ozon.ru:443/api/client/statistics/phrases',
                        'attribution': 'https://performance.ozon.ru:443/api/client/statistics/attribution',
                        'media': 'https://performance.ozon.ru:443/api/client/statistics/campaign/media',
                        'product': 'https://performance.ozon.ru:443/api/client/statistics/campaign/product',
                        'daily': 'https://performance.ozon.ru:443/api/client/statistics/daily',
                        'traffic': 'https://performance.ozon.ru:443/api/client/vendors/statistics'}
        self.day_lim = day_lim
        self.camp_lim = camp_lim

        try:
            self.auth = self.get_token()
        except:
            self.auth = None
            print('Нет доступа к серверу')

        try:
            self.campaigns = [camp['id'] for camp in self.get_campaigns()]
            self.objects = {}
            for camp in self.campaigns:
                self.objects[camp] = [obj['id'] for obj in self.get_objects(campaign_id=camp)]
        except:
            # self.campaigns = None
            # self.objects = None
            print('Ошибка при получении кампаний')

        self.st_camp = []
        self.st_ph = []
        self.st_attr = []
        self.st_med = None
        self.st_pr = None
        self.st_dai = None

    def get_token(self):
        url = 'https://performance.ozon.ru/api/client/token'
        head = {"Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials"
                }
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Подключение успешно, токен получен')
            return response.json()
        else:
            print(response.text)
            return None

    def get_campaigns(self):
        """
        Возвращает список кампаний
        """
        url = 'https://performance.ozon.ru:443/api/client/campaign'
        head = {"Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": self.auth['token_type'] + ' ' + self.auth['access_token']
                }
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            print(f"Найдено {len(response.json()['list'])} кампаний")
            return response.json()['list']
        else:
            print(response.text)
            return None

    def get_objects(self, campaign_id):
        """
        Возвращает список рекламируемых объектов в кампании
        """
        url = f"https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/objects"
        head = {"Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": self.auth['token_type'] + ' ' + self.auth['access_token']
                }
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response.json()['list']
        else:
            print(response.text)

    def split_data(self, camp_lim):
        """
        Разбивает данные в соответствии с ограничениями Ozon
        """
        if len(self.objects) >= camp_lim:
            data = []
            for i in range(0, len(self.objects), camp_lim):
                data.append(dict(list(self.objects.items())[i:i + camp_lim]))
        else:
            data = [self.objects]
        return data

    def split_time(self, date_from, date_to, day_lim):
        """
        Разбивает временной промежуток в соответствии с лимитом Ozon
        """
        delta = datetime.strptime(date_to, '%Y-%m-%d') - datetime.strptime(date_from, '%Y-%m-%d')
        if delta.days > day_lim:
            tms = []
            for t in range(0, delta.days, day_lim):
                dt_fr = str((datetime.strptime(date_from, '%Y-%m-%d') + timedelta(days=t)).date())
                if (datetime.strptime(date_from, '%Y-%m-%d') + timedelta(days=t + day_lim - 1)).date() >= \
                        (datetime.strptime(date_to, '%Y-%m-%d')).date():
                    dt_to = str((datetime.strptime(date_to, '%Y-%m-%d')).date())
                else:
                    dt_to = str((datetime.strptime(date_from, '%Y-%m-%d') + timedelta(days=t + day_lim - 1)).date())
                tms.append([dt_fr, dt_to])
        else:
            tms = [[date_from, date_to]]

        return tms

    def get_statistics(self, campaigns,
                       t_date_from=None,
                       t_date_to=None,
                       group_by="DATE",
                       n_attempts=5,
                       delay=5):
        """
        Возвращает статистику по кампании

        DATE — группировка по дате (по дням);
        START_OF_WEEK — группировка по неделям;
        START_OF_MONTH — группировка по месяцам.

        """
        url = self.methods['statistics']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"campaigns": campaigns,
                "dateFrom": t_date_from,
                "dateTo": t_date_to,
                "groupBy": group_by
                }

        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Статистика по кампаниям получена')
            if len(campaigns) == 1:
                return [response.json()['UUID'], 'csv']
            else:
                return [response.json()['UUID'], 'zip']
        elif response.status_code == 429:
            n = 0
            while n < n_attempts:
                time.sleep(delay)
                response = requests.post(url, headers=head, data=json.dumps(body))
                print('statistics, статус', response.status_code)
                # print(response.headers)
                if response.status_code == 200:
                    print('Статистика по кампаниям получена')
                    if len(campaigns) == 1:
                        return [response.json()['UUID'], 'csv']
                    else:
                        return [response.json()['UUID'], 'zip']
                    # break
                else:
                    n += 1
        else:
            print(response.text)

    def get_phrases(self, objects,
                    t_date_from=None,
                    t_date_to=None,
                    group_by="DATE",
                    n_attempts=5,
                    delay=3):
        """
        Возвращает отчет по фразам
        """
        url = self.methods['phrases']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        res = []
        for camp, obj in objects.items():
            if len(obj) != 0:
                body = {"campaigns": [camp],
                        "objects": obj,
                        "dateFrom": t_date_from,
                        "dateTo": t_date_to,
                        "groupBy": group_by
                        }
                response = requests.post(url, headers=head, data=json.dumps(body))
                if response.status_code == 200:
                    print('Статистика по фразам получена')
                    res.append([response.json()['UUID'], 'csv'])
                elif response.status_code == 429:
                    n = 0
                    while n < n_attempts:
                        time.sleep(delay)
                        response = requests.post(url, headers=head, data=json.dumps(body))
                        print('phrases, статус', response.status_code)
                        if response.status_code == 200:
                            print('Статистика по фразам получена')
                            res.append([response.json()['UUID'], 'csv'])
                            break
                        else:
                            n += 1
                else:
                    print(response.text)
        return res

    def get_attribution(self, campaigns,
                        t_date_from=None,
                        t_date_to=None,
                        group_by="DATE",
                        n_attempts=5,
                        delay=3):
        """
        Возвращает отчёт по заказам
        """
        url = self.methods['attribution']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"campaigns": campaigns,
                "dateFrom": t_date_from,
                "dateTo": t_date_to,
                "groupBy": group_by
                }
        time.sleep(delay)
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Статистика по заказам получена')
            if len(campaigns) == 1:
                return [response.json()['UUID'], 'csv']
            else:
                return [response.json()['UUID'], 'zip']
        elif response.status_code == 429:
            n = 0
            while n < n_attempts:
                time.sleep(delay)
                response = requests.post(url, headers=head, data=json.dumps(body))
                print('attribution, статус', response.status_code)
                if response.status_code == 200:
                    print('Статистика по заказам получена')
                    if len(campaigns) == 1:
                        return [response.json()['UUID'], 'csv']
                    else:
                        return [response.json()['UUID'], 'zip']
                    # break
                else:
                    n += 1
        else:
            print(response.text)

    def get_media(self, campaigns,
                  t_date_from=None,
                  t_date_to=None):
        """
        Возвращает статистику по медийным кампаниям
        """
        url = self.methods['media']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        params = {"campaigns": campaigns,
                  "dateFrom": t_date_from,
                  "dateTo": t_date_to
                  }
        response = requests.get(url, headers=head, params=params)
        if response.status_code == 200:
            print('Статистика по медиа получена')
            return response
        else:
            print(response.text)

    def get_product(self, campaigns,
                    t_date_from=None,
                    t_date_to=None):
        """
        Возвращает статистику по продуктовым кампаниям
        """
        url = self.methods['product']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json"
                }
        params = {"campaigns": campaigns,
                  "dateFrom": t_date_from,
                  "dateTo": t_date_to
                  }
        response = requests.get(url, headers=head, params=params)
        if response.status_code == 200:
            print('Статистика продуктовая получена')
            return response
        else:
            print(response.text)

    def get_daily(self, campaigns,
                  t_date_from=None,
                  t_date_to=None):
        """
        Возвращает дневную статистику по кампаниям
        """
        url = self.methods['daily']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json"
                }
        params = {"campaigns": campaigns,
                  "dateFrom": t_date_from,
                  "dateTo": t_date_to
                  }
        response = requests.get(url, headers=head, params=params)
        if response.status_code == 200:
            print('Статистика дневная получена')
            return response
        else:
            print(response.text)

    def get_traffic(self, t_date_from, t_date_to, type="TRAFFIC_SOURCES"):
        """
        Метод для запуска формирования отчёта с аналитикой внешнего трафика
        TRAFFIC_SOURCES — отчёт по источникам трафика
        ORDERS — отчёт по заказам
        """
        url = self.methods['traffic']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json"
                #                "Accept": "application/json"
                }
        body = {"dateFrom": t_date_from,
                "dateTo": t_date_to,
                "type": type
                }
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Аналитика трафика получена')
            return response.json()['UUID']
        else:
            print(response.text)

    def traffic_list(self):
        """
        Список запрошенных отчётов с аналитикой внешнего трафика
        """
        url = 'https://performance.ozon.ru:443/api/client/vendors/statistics/list'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json"
                }
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response.json()['items']
        else:
            print(response.text)

    def status_traffic(self, uuid):
        """
        Возвращает информацию об отчёте
        """
        url = 'https://performance.ozon.ru:443/api/client/vendors/statistics/' + uuid
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json"
                }
        params = {'vendor': 'true'}
        response = requests.get(url, headers=head, params=params)
        # print(response.status_code)
        if response.status_code == 200:
            return response.json()
        else:
            print(response.text)

    def get_traffic_report(self, uuid):
        """
        Получить файл отчета
        """
        url = f'https://performance.ozon.ru:443/api/client/statistics/report?UUID={uuid}&vendor=t'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token']}
        response = requests.get(url, headers=head)
        print(response.status_code)
        if response.status_code == 200:
            return response
        else:
            print(response.text)

    def status_report(self, uuid):
        """
        Возвращает статус отчета
        """
        url = 'https://performance.ozon.ru:443/api/client/statistics/' + uuid
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response
        else:
            print(response.text)

    def get_report(self, uuid):
        """
        Получить файл отчета
        """
        url = 'https://performance.ozon.ru:443/api/client/statistics/report?UUID=' + uuid
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token']}
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response
        else:
            print(response.text)

    def collect_data(self, date_from, date_to,
                     statistics=False, phrases=False, attribution=False, media=False, product=False, daily=False,
                     traffic=False):
        data = self.split_data(camp_lim=self.camp_lim)
        time_ = self.split_time(date_from=date_from, date_to=date_to, day_lim=self.day_lim)
        self.time = time_
        self.date_from = date_from
        self.date_to = date_to
        if statistics is True:
            self.st_camp = []
        if phrases is True:
            self.st_ph = []
        if attribution is True:
            self.st_attr = []
        if media is True:
            self.st_med = self.get_media(self.campaigns, t_date_from=date_from, t_date_to=date_to)
        if product is True:
            self.st_pr = self.get_product(self.campaigns, t_date_from=date_from, t_date_to=date_to)
        if daily is True:
            self.st_dai = self.get_daily(self.campaigns, t_date_from=date_from, t_date_to=date_to)
        if traffic is True:
            self.st_trf = self.get_traffic(t_date_from=date_from, t_date_to=date_to)
        try:
            for d in data:
                for t in time_:
                    if statistics is True:
                        self.st_camp.append(self.get_statistics(list(d.keys()), t_date_from=t[0], t_date_to=t[1]))
                    if phrases is True:
                        self.st_ph.append(self.get_phrases(d, t_date_from=t[0], t_date_to=t[1]))
                    if attribution is True:
                        self.st_attr.append(self.get_attribution(list(d.keys()), t_date_from=t[0], t_date_to=t[1]))
        except TimeoutError:
            print('Нет ответа от сервера')

    def save_data(self, path_,
                  statistics=False, phrases=False, attribution=False, media=False, product=False, daily=False,
                  traffic=False):
        #         folder = path_
        folder = path_ + f'{self.account_id}-{self.client_id}/'
        if not os.path.isdir(folder):
            os.mkdir(folder)
        if media is True:
            if not os.path.isdir(folder + 'media'):
                os.mkdir(folder + 'media')
            name = folder + r'media/' + f"media_{self.date_from}-{self.date_to}.csv"
            file = open(name, 'wb')
            file.write(self.st_med.content)
            file.close()
            print('Сохранен', name)
        if product is True:
            if not os.path.isdir(folder + 'product'):
                os.mkdir(folder + 'product')
            name = folder + r'product/' + f"product_{self.date_from}-{self.date_to}.csv"
            file = open(name, 'wb')
            file.write(self.st_pr.content)
            file.close()
            print('Сохранен', name)
        if daily is True:
            if not os.path.isdir(folder + 'daily'):
                os.mkdir(folder + 'daily')
            name = folder + r'daily/' + f"daily_{self.date_from}-{self.date_to}.csv"
            file = open(name, 'wb')
            file.write(self.st_dai.content)
            file.close()
            print('Сохранен', name)
        if traffic is True:
            if not os.path.isdir(folder + 'traffic'):
                os.mkdir(folder + 'traffic')
            status = ''
            while status != 'OK':
                time.sleep(5)
                status = self.status_traffic(uuid=self.st_trf)['state']
                print(status)
            report = self.get_traffic_report(uuid=self.st_trf)
            name = folder + r'traffic/' + f"traffic_{self.date_from}-{self.date_to}.xlsx"
            file = open(name, 'wb')
            file.write(report.content)
            file.close()
            print('Сохранен', name)
        if statistics is True:
            if not os.path.isdir(folder + 'statistics'):
                os.mkdir(folder + 'statistics')
            for num, camp in enumerate(self.st_camp):
                try:
                    status = ''
                    while status != 'OK':
                        time.sleep(10)
                        status = self.status_report(uuid=camp[0]).json()['state']
                        print(status)
                    report = self.get_report(uuid=camp[0])
                    name = folder + r'statistics/' + f"campaigns_{num}.{camp[1]}"
                    file = open(name, 'wb')
                    file.write(report.content)
                    file.close()
                    print('Сохранен', name)
                except:
                    continue
        if phrases is True:
            if not os.path.isdir(folder + 'phrases'):
                os.mkdir(folder + 'phrases')
            for num, ph in enumerate(self.st_ph):
                try:
                    for n_camp, phrases in enumerate(ph):
                        try:
                            status = ''
                            while status != 'OK':
                                time.sleep(10)
                                status = self.status_report(uuid=phrases[0]).json()['state']
                                print(status)
                            report = self.get_report(uuid=phrases[0])
                            name = folder + r'phrases/' + f"phrases_{num}_{n_camp}.{phrases[1]}"
                            file = open(name, 'wb')
                            file.write(report.content)
                            file.close()
                            print('Сохранен', name)
                        except:
                            continue
                except:
                    continue
        if attribution is True:
            if not os.path.isdir(folder + 'attribution'):
                os.mkdir(folder + 'attribution')
            for num, attr in enumerate(self.st_attr):
                try:
                    status = ''
                    while status != 'OK':
                        time.sleep(10)
                        status = self.status_report(uuid=attr[0]).json()['state']
                        print(status)
                    report = self.get_report(uuid=attr[0])
                    name = folder + r'attribution/' + f"attr_{num}.{attr[1]}"
                    file = open(name, 'wb')
                    file.write(report.content)
                    file.close()
                    print('Сохранен', name)
                except:
                    continue

    def get_camp_modes(self):
        """
        Доступные режимы создания рекламных кампаний
        """
        url = 'https://performance.ozon.ru:443/api/client/campaign/available'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        response = requests.get(url, headers=head)
        return response

    def create_camp(self, title, from_date, to_date, daily_budget,
                    exp_strategy="DAILY_BUDGET",
                    placement="PLACEMENT_INVALID",
                    pcm="PRODUCT_CAMPAIGN_MODE_AUTO"):
        """
        Метод для создания товарной рекламной кампании с моделью оплаты за показы
        https://docs.ozon.ru/api/performance/#operation/CreateProductCampaignCPM
        """
        url = 'https://performance.ozon.ru:443/api/client/campaign/cpm/product'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"title": title,
                "fromDate": from_date,
                "toDate": to_date,
                "dailyBudget": str(daily_budget),
                "expenseStrategy": exp_strategy,
                "placement": placement,
                "productCampaignMode": pcm
                }
        response = requests.post(url, headers=head, data=json.dumps(body))
        return response

    def create_camp_cpm(self,
                        title=None,
                        from_date=None,
                        to_date=None,
                        budget='0',
                        daily_budget=None,
                        exp_strategy=None,
                        placement="PLACEMENT_INVALID",
                        product_autopilot_strategy=None,
                        autopilot_category_id=None,
                        autopilot_sku_add_mode=None,
                        pcm=None):
        """
        Метод для создания товарной рекламной кампании с моделью оплаты за показы
        https://docs.ozon.ru/api/performance/#operation/CreateProductCampaignCPM
        """
        url = 'https://performance.ozon.ru:443/api/client/campaign/cpm/product'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"placement": placement}
        if title is not None:
            body.setdefault('title', title)

        if from_date is not None:
            body.setdefault('from_date', from_date)

        if to_date is not None:
            body.setdefault('to_date', to_date)

        if budget is not None:
            body.setdefault('budget', budget)

        if daily_budget is not None:
            body.setdefault('daily_budget', daily_budget)

        if exp_strategy is not None:
            body.setdefault('exp_strategy', exp_strategy)

        if product_autopilot_strategy is not None:
            body.setdefault('product_autopilot_strategy', product_autopilot_strategy)

            if autopilot_category_id is not None and autopilot_sku_add_mode is not None:
                body.setdefault('autopilot')
                body['autopilot'] = {"categoryId": autopilot_category_id, "skuAddMode": autopilot_sku_add_mode}

        if pcm is not None:
            body.setdefault('pcm', pcm)

        response = requests.post(url, headers=head, data=json.dumps(body))
        return response

    def create_camp_cpc(self,
                        title=None,
                        from_date=None,
                        to_date=None,
                        daily_budget=None,
                        exp_strategy=None,
                        placement="PLACEMENT_INVALID",
                        product_autopilot_strategy="NO_AUTO_STRATEGY",
                        pcm="PRODUCT_CAMPAIGN_MODE_AUTO"):
        """
        Метод для создания рекламной кампании с моделью оплаты за клики
        https://docs.ozon.ru/api/performance/#operation/CreateProductCampaignCPC
        """
        url = 'https://performance.ozon.ru:443/api/client/campaign/cpc/product'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"placement": placement}
        if title is not None:
            body.setdefault('title', title)
        if from_date is not None:
            body.setdefault('from_date', from_date)
        if to_date is not None:
            body.setdefault('to_date', to_date)
        if daily_budget is not None:
            body.setdefault('daily_budget', daily_budget)
        if exp_strategy is not None:
            body.setdefault('exp_strategy', exp_strategy)
        if product_autopilot_strategy is not None:
            body.setdefault('product_autopilot_strategy', product_autopilot_strategy)
        if pcm is not None:
            body.setdefault('pcm', pcm)

        response = requests.post(url, headers=head, data=json.dumps(body))
        return response

    def camp_activate(self, campaign_id):
        """
        Активировать рекламную кампанию
        """
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/activate'
        response = requests.post(url, headers=head)
        return response

    def camp_deactivate(self, campaign_id):
        """
        Деактивировать рекламную кампанию
        """
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }

        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/deactivate'
        response = requests.post(url, headers=head)
        return response

    def camp_period(self, campaign_id, date_from=None, date_to=None
                    #                     daily_budget,
                    #                     exp_str='DAILY_BUDGET'
                    ):
        """
        Метод для изменения сроков проведения кампании
        Способ распределения бюджета:
        DAILY_BUDGET — бюджет равномерно распределяется по дням;
        ASAP — быстрая открутка, бюджет не ограничен по дням.
        """
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/period'

        body = dict()

        if date_from is not None:
            body.setdefault("fromDate", date_from)

        if date_to is not None:
            body.setdefault("toDate", date_to)

        # body = {"fromDate": date_from,
        #         "toDate": date_to
        #         }
        response = requests.put(url, headers=head, data=json.dumps(body))
        return response

    def camp_budget(self, campaign_id,
                    daily_budget,
                    exp_str=None
                    ):
        """
        Метод для изменения ограничения дневного бюджета кампании
        Способ распределения бюджета:
        DAILY_BUDGET — бюджет равномерно распределяется по дням;
        ASAP — быстрая открутка, бюджет не ограничен по дням.
        """
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/daily_budget'

        body = {"dailyBudget": daily_budget}

        if exp_str is not None:
            body.setdefault("expenseStrategy", exp_str)

        response = requests.put(url, headers=head, data=json.dumps(body))
        return response

    @staticmethod
    def card_bids(sku_list: list, bids_list=None, lim=500):
        """
        Для добавления в кампанию товаров с размещением в карточке товара
        Для обновления ставок у товаров в рекламной кампании с размещением в карточке товара
        """
        if bids_list is not None:
            sku_list = sku_list[:lim]
            bids_list = bids_list[:lim]
            if len(sku_list) == len(bids_list):
                return [{'sku': a, 'bid': float(b) * 1e6} for a, b in zip(sku_list, bids_list)]
            else:
                print('Не правильный формат данных')
                return None
        else:
            sku_list = sku_list[:lim]
            return [{'sku': a, 'bid': None} for a in sku_list]

    @staticmethod
    def group_bids(sku_list, groups_list, bids_list=None, lim=500):
        """
        Для добавления в кампанию товаров в ранее созданные группы с размещением на страницах каталога и поиска
        """
        if sku_list is not None and groups_list is not None and bids_list is not None:
            sku_list = sku_list[:lim]
            bids_list = bids_list[:lim]
            groups_list = groups_list[:lim]

            if len(sku_list) == len(groups_list) == len(bids_list):
                return [{'sku': a, 'bid': b, 'groupId': c} for a, b, c in zip(sku_list, bids_list, groups_list)]
            else:
                print('Не правильный формат данных')
                return None

        elif sku_list is not None and groups_list is not None and bids_list is None:
            sku_list = sku_list[:lim]
            groups_list = groups_list[:lim]

            if len(sku_list) == len(groups_list):
                return [{'sku': a, 'bid': None, 'groupId': c} for a, c in zip(sku_list, groups_list)]
            else:
                print('Не правильный формат данных')
                return None
        else:
            print('Не правильный формат данных')
            return None

    @staticmethod
    def phrases_bid(sku: str, stopwords: list, phrases: list, bids_list=None):
        """
        Для добавления (обновления) в кампанию товара без группы с размещением на страницах каталога и поиска
        """
        if bids_list is not None and len(phrases) == len(bids_list):
            return [{'sku': sku,
                     'stopWords': stopwords,
                     'phrases': [{'phrase': a, 'bid': b} for a, b in zip(phrases, bids_list)]
                     }]
        elif bids_list is None:
            return [{'sku': sku,
                     'stopWords': stopwords,
                     'phrases': [{'phrase': a, 'bid': None} for a in phrases]
                     }]
        else:
            print('Не правильный формат данных')
            return None

    @staticmethod
    def phrases_bids(sku_list: list[str],
                     phrases: list[str],
                     bids_list: list[str] = None,
                     phrases_bids: list[str] = None,
                     stopwords: list[str] = None
                     ):
        """Для добавления (обновления) в кампанию товаров без группы с размещением на страницах каталога и поиска"""

        if phrases_bids is not None:
            if len(phrases_bids) == len(phrases):
                phrases_params = [{"bid": bd, "phrase": phr} for bd, phr in zip(phrases_bids, phrases)]
            else:
                return None
        else:
            phrases_params = [{"phrase": phr} for phr in phrases]

        if bids_list is not None:
            if len(sku_list) == len(bids_list):
                if stopwords is not None:
                    return [{"sku": sku, "bid": bid, "phrases": phrases_params, "stopWords": stopwords} for sku, bid in zip(sku_list, bids_list)]
                else:
                    return [{"sku": sku, "bid": bid, "phrases": phrases_params} for sku, bid in zip(sku_list, bids_list)]
            else:
                return None

        else:
            if stopwords is not None:
                return [{"sku": sku, "phrases": phrases_params,"stopWords": stopwords} for sku in sku_list]
            else:
                return[{"sku": sku, "phrases": phrases_params} for sku in sku_list]

    def add_products(self, campaign_id, bids):
        """
        Добавить товары в кампанию
        """
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/products'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"bids": bids}
        response = requests.post(url, headers=head, data=json.dumps(body))
        return response

    def upd_bids(self, campaign_id, bids):
        """
        Обновить ставки товаров
        """
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/products'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"bids": bids}
        response = requests.put(url, headers=head, data=json.dumps(body))
        return response

    def prod_list(self, campaign_id):
        """
        Список товаров кампании
        """
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/products'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Accept": "application/json"
                }
        response = requests.get(url, headers=head)
        return response

    def del_products(self, campaign_id, sku_list: list):
        """
        Удалить товары из кампании
        """
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/products/delete'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"sku": sku_list}
        response = requests.post(url, headers=head, data=json.dumps(body))
        return response

    def add_group(self, campaign_id: str,
                  title: str = None,
                  stopwords: list[str] = None,
                  phrases: list[str] = None,
                  bids_list: list[str] = None,
                  relevance_status: list[str] = None
                  ):
        """
        Создать группу
        """
        url = f"""https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/group"""
        # url = f"""https://performance.ozon.ru:443/api/client/campaign/group"""

        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        if phrases is not None and bids_list is not None and relevance_status is not None and len(phrases) == len(
                bids_list) == len(relevance_status):
            phrases_list = [{'phrase': a, 'bid': b, 'relevanceStatus': c} for a, b, c in zip(phrases, bids_list,
                                                                                             relevance_status)]
        else:
            phrases_list = None

        body = dict()
        # body = {'campaignId': campaign_id}

        if title is not None:
            body.setdefault("title", title)

        if stopwords is not None:
            body.setdefault("stopWords", stopwords)

        if phrases_list is not None:
            body.setdefault("phrases", phrases_list)

        response = requests.post(url, headers=head, data=json.dumps(body))

        # print(url)
        # print(body)

        return response

    def edit_group(self, campaign_id: str,
                   group_id: str,
                   title=None,
                   stopwords=None,
                   phrases=None,
                   bids_list=None,
                   relevance_status=None
                   ):
        """
        Редактировать группу
        """
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/group/{group_id}'

        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }

        if phrases is not None and bids_list is not None and relevance_status is not None and len(phrases) == len(
                bids_list) == len(relevance_status):
            phrases_list = [{'phrase': a, 'bid': b, 'relevanceStatus': c} for a, b, c in zip(phrases, bids_list,
                                                                                             relevance_status)]
        else:
            phrases_list = None

        body = dict()

        if title is not None:
            body.setdefault("title", title)

        if stopwords is not None:
            body.setdefault("stopWords", stopwords)

        if phrases_list is not None:
            body.setdefault("phrases", phrases_list)

        response = requests.put(url, headers=head, data=json.dumps(body))
        return response


class DbWorking:
    def __init__(self,
                 db_access,
                 data_table_name='analitics_data2'
                 ):

        self.db_access = db_access
        self.data_table_name = data_table_name
        # необходимые запросы к БД
        self.api_keys_resp = 'SELECT * FROM account_list'
        self.keys_dt_cols_resp = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'account_list'"
        self.an_dt_resp = f"SELECT * FROM {data_table_name}"
        self.an_dt_cols_resp = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'analitics_data2'"
        self.product_list_resp = 'SELECT * FROM product_list'
        self.api_perf_keys_resp = "select max(id),foo.client_id_performance, client_secret_performance\
                                    from (select distinct(client_id_performance) from account_list) as foo\
                                    join account_list\
                                    on foo.client_id_performance = account_list.client_id_performance\
                                    where (status_2 = 'Active' or status_1 = 'Active') and mp_id = 1\
                                    group by foo.client_id_performance, client_secret_performance\
                                    order by client_id_performance"
        self.api_perf_keys_resp2 = """
                    SELECT al.id, asd.attribute_value key_attribute_value, asd2.attribute_value
                    FROM account_service_data asd
                    JOIN account_list al ON asd.account_id = al.id
                    JOIN (SELECT al.mp_id, asd.account_id, asd.attribute_id, asd.attribute_value 
                    FROM account_service_data asd
                    JOIN account_list al ON asd.account_id = al.id WHERE al.mp_id = 14) asd2 
                    ON asd2.mp_id = al.mp_id 
                    AND asd2.account_id= asd.account_id AND asd2.attribute_id <> asd.attribute_id
                    WHERE al.mp_id = 14 
                    AND asd.attribute_id = 9
                    AND al.status_1 = 'Active'
                    GROUP BY asd.attribute_id, asd.attribute_value, asd2.attribute_id, asd2.attribute_value, al.id
                    ORDER BY id
                    """

    def test_db_connection(self):
        """
        Проверка доступа к БД
        """
        try:
            conn = psycopg2.connect(self.db_access)
            q = conn.cursor()
            q.execute('SELECT version()')
            connection = q.fetchone()
            print(connection)
            conn.close()
            return connection
        except:
            print('Нет подключения к БД')
            return None

    def get_analitics_data(self):
        """
        Загружает таблицу из базы
        """
        print('Загружается analitics_data')
        self.db_data = pd.read_sql(self.an_dt_resp, psycopg2.connect(self.db_access))
        print('Загружена analitics_data')

    def get_analitics_data2(self, db_params):
        """
        Загружает таблицу из базы
        """
        print('Загружается analitics_data')
        engine = create_engine(db_params)
        self.db_data = pd.read_sql(self.an_dt_resp, con=engine)
        print('Загружена analitics_data')

    def get_last_date(self):
        """
        Возвращает последнюю дату записи в базе
        """
        db_data = self.db_data
        return db_data['data'].sort_values(ascending=False).values[0]

    def get_keys(self):
        """
        Загружает из базы таблицу ключей
        """
        try:
            df = pd.read_sql(self.api_keys_resp, psycopg2.connect(self.db_access))
            print('Загружены api_keys')
            return df
        except:
            print('Доступ к таблице запрещен')
            return None

    def get_perf_keys(self):
        """
        Загружает ключи performance
        """
        try:
            df = pd.read_sql(self.api_perf_keys_resp, psycopg2.connect(self.db_access))
            print('Загружены performance_api_keys')
            return df
        except:
            print('Доступ к таблице запрещен')
            return None

    def get_perf_keys2(self, db_params):
        """
        Загружает ключи performance
        """
        try:
            engine = create_engine(db_params)
            df = pd.read_sql(self.api_perf_keys_resp2, con=engine)
            print('Загружены performance_api_keys')
            return df
        except:
            print('Доступ к таблице запрещен')
            return None

    @staticmethod
    def extract_zips(path_, rem=False):
        """
        Распаковывает все zip в папках statistics папок аккаунтов
        """
        for folder in os.listdir(path_):
            zip_files = glob.glob(os.path.join(path_ + folder + r'/statistics', "*.zip"))
            for file in zip_files:
                print(f'Распаковка {file}')
                with zipfile.ZipFile(file) as zf:
                    zf.extractall(path_ + folder + r'/statistics')
                if rem is True:
                    os.remove(file)
                    print(f'Удаление {file}')

    @staticmethod
    def stat_read_trans(file, api_id=None, account_id=None):
        """
        Обрабатывает датасет
        """
        data = pd.read_csv(file, sep=';')
        data = data.reset_index()

        camp = data.keys()[-1].split(',')[0].split()[-1]
        data.columns = data[0:1].values.tolist()[0]
        data.drop(index=0, inplace=True)
        data.drop(data.tail(1).index, inplace=True)

        data['api_id'] = api_id
        data['account_id'] = account_id
        data['Кампания'] = camp

        data = data[data.columns[-1:].tolist() + data.columns[:-1].tolist()]
        data = data[data.columns.dropna()]
        data = data.dropna(axis=0, how='any', thresh=10)
        return data

    def make_dataset(self, path_):
        """
        Собирает датасет
        """
        stat_data = []
        for folder in os.listdir(path_):
            csv_files = glob.glob(os.path.join(path_ + folder + r'/statistics', "*.csv"))
            for file in csv_files:
                try:
                    account_id = os.path.dirname(file).split('/')[-2].split('-')[0]
                    api_id = os.path.dirname(file).split('/')[-2].split('-')[1]
                    stat_data.append(self.stat_read_trans(file, api_id=api_id, account_id=account_id))
                except IndexError:
                    continue
        dataset = pd.concat(stat_data, axis=0).reset_index().drop('index', axis=1)
        dataset['data'] = dataset[['Дата', 'День']].fillna('nan').apply(lambda x: x[0] if x[1] == 'nan' else x[1],
                                                                        axis=1)
        dataset.drop(columns=['Дата', 'День'], inplace=True)
        dataset['name'] = dataset[['Наименование', 'Название товара']].fillna('nan').apply(
            lambda x: x[0] if x[1] == 'nan' else x[1], axis=1)
        dataset.drop(columns=['Наименование', 'Название товара'], inplace=True)
        dataset['orders'] = dataset[['Количество', 'Заказы']].fillna('').apply(lambda x: x[0] if x[1] == '' else x[1],
                                                                               axis=1)
        dataset.drop(columns=['Количество', 'Заказы'], inplace=True)
        dataset['price'] = dataset[['Цена продажи', 'Цена товара (руб.)']].fillna('').apply(
            lambda x: x[0] if x[1] == '' else x[1], axis=1)
        dataset.drop(columns=['Цена продажи', 'Цена товара (руб.)'], inplace=True)
        dataset['revenue'] = dataset[['Выручка (руб.)', 'Стоимость, руб.']].fillna('').apply(
            lambda x: x[0] if x[1] == '' else x[1], axis=1)
        dataset.drop(columns=['Выручка (руб.)', 'Стоимость, руб.'], inplace=True)
        dataset['expense'] = dataset[['Расход (руб., с НДС)', 'Расход, руб.']].fillna('').apply(
            lambda x: x[0] if x[1] == '' else x[1], axis=1)
        dataset.drop(columns=['Расход (руб., с НДС)', 'Расход, руб.'], inplace=True)

        dataset.rename(columns={'ID заказа': 'order_id', 'Номер заказа': 'order_number', 'Ozon ID': 'ozon_id',
                                'Ozon ID рекламируемого товара': 'ozon_id_ad_sku', 'Артикул': 'articul',
                                'Ставка, %': 'search_price_perc', 'Ставка, руб.': 'search_price_rur',
                                'Тип страницы': 'pagetype', 'Условие показа': 'viewtype', 'Показы': 'views',
                                'Клики': 'clicks', 'CTR (%)': 'ctr', 'Средняя ставка за 1000 показов (руб.)': 'cpm',
                                'Заказы модели': 'orders_model', 'Выручка с заказов модели (руб.)': 'revenue_model',
                                'Тип условия': 'request_type', 'Платформа': 'platfrom', 'Охват': 'audience',
                                'Баннер': 'banner', 'Средняя ставка (руб.)': 'avrg_bid', 'Кампания': 'actionnum',
                                'Расход за минусом бонусов (руб., с НДС)': 'exp_bonus'}, inplace=True)

        dataset['data'] = dataset['data'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').date())
        #        dataset['sku'] = dataset['sku'].fillna('nan')

        for col in dataset.columns:
            if self.db_data[col].dtypes == 'float64' or self.db_data[col].dtypes == 'int64':
                dataset[col] = dataset[col].str.replace(',', '.')
                dataset[col] = dataset[col].replace(r'^\s*$', np.nan, regex=True)
                dataset[col] = dataset[col].astype(self.db_data[col].dtypes)

        return dataset

    @staticmethod
    def stat_read_trans2(file, api_id=np.nan, account_id=np.nan):
        """
        Обрабатывает датасет
        """
        data = pd.read_csv(file, sep=';', header=1,
                           skipfooter=1, engine='python'
                           )
        # data = data.dropna(axis=0, how='any', thresh=10)
        data = data.dropna(axis=0, thresh=10)
        camp = pd.read_csv(file, sep=';', header=0, nrows=0).columns[-1].split(',')[0].split()[-1]

        data['api_id'] = api_id
        data['account_id'] = account_id
        data['actionnum'] = camp

        data.rename(columns={'ID заказа': 'order_id',
                             'Номер заказа': 'order_number',
                             'Ozon ID': 'ozon_id',
                             'Ozon ID рекламируемого товара': 'ozon_id_ad_sku',
                             'Артикул': 'articul',
                             'Ставка, %': 'search_price_perc',
                             'Ставка, руб.': 'search_price_rur',
                             'Тип страницы': 'pagetype',
                             'Условие показа': 'viewtype',
                             'Показы': 'views',
                             'Клики': 'clicks',
                             'CTR (%)': 'ctr',
                             'Средняя ставка за 1000 показов (руб.)': 'cpm',
                             'Заказы модели': 'orders_model',
                             'Выручка с заказов модели (руб.)': 'revenue_model',
                             'Тип условия': 'request_type',
                             'Платформа': 'platfrom',
                             'Охват': 'audience',
                             'Баннер': 'banner',
                             'Средняя ставка (руб.)': 'avrg_bid',
                             'Расход за минусом бонусов (руб., с НДС)': 'exp_bonus',
                             'Дата': 'data',
                             'День': 'data',
                             'Наименование': 'name',
                             'Название товара': 'name',
                             'Количество': 'orders',
                             'Заказы': 'orders',
                             'Цена продажи': 'price',
                             'Цена товара (руб.)': 'price',
                             'Выручка (руб.)': 'revenue',
                             'Стоимость, руб.': 'revenue',
                             'Расход (руб., с НДС)': 'expense',
                             'Расход, руб.': 'expense',
                             'Unnamed: 1': 'empty',
                             'Средняя ставка за клик (руб.)': 'cpc',
                             'Ср. цена 1000 показов, ₽': 'cpm',
                             'Расход, ₽, с НДС': 'expense',
                             'Цена товара, ₽': 'price',
                             'Выручка, ₽': 'revenue',
                             'Выручка с заказов модели, ₽': 'revenue_model',
                             'Стоимость, ₽': 'revenue',
                             'Ставка, ₽': 'search_price_rur',
                             'Расход, ₽': 'expense',
                             'Средняя ставка, ₽': 'avrg_bid',
                             'Расход за минусом бонусов, ₽, с НДС': 'exp_bonus',
                             'Ср. цена клика, ₽': 'cpc',
                             'Средняя ставка (руб.)%!(EXTRA string=₽)': 'avrg_bid'
                             }, inplace=True)

        data['data'] = data['data'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').date())
        #     print(data.shape)
        return data

    def make_dataset2(self, path_):
        """
        Собирает датасет
        """
        stat_data = []
        for folder in os.listdir(path_):
            csv_files = glob.glob(os.path.join(path_ + folder + r'/statistics', "*.csv"))
            for file in csv_files:
                try:
                    account_id = os.path.dirname(file).split('/')[-2].split('-')[0]
                    api_id = os.path.dirname(file).split('/')[-2].split('-')[1]
                    stat_data.append(self.stat_read_trans2(file, api_id=api_id, account_id=account_id))
                except IndexError:
                    continue

        dataset = pd.concat(stat_data, axis=0).reset_index().drop('index', axis=1)

        for col in dataset.columns:
            # print(col, working.db_data[col].dtypes, dataset[col].dtypes)
            if self.db_data[col].dtypes != dataset[col].dtypes:
                if (self.db_data[col].dtypes == 'float64' or self.db_data[col].dtypes == 'int64') and dataset[col].dtypes == 'object':
                    dataset[col] = dataset[col].astype(str).str.replace(',', '.')
                    dataset[col] = dataset[col].replace(r'^\s*$', np.nan, regex=True)
                dataset[col] = dataset[col].astype(self.db_data[col].dtypes)

        return dataset

    @staticmethod
    def rem_csv(path_):
        """
        Удаляет файлы
        """
        for folder in os.listdir(path_):
            csv_files = glob.glob(os.path.join(path_ + folder + r'/statistics', "*.csv"))
            for file in csv_files:
                os.remove(file)
                print(f'Удаление {file}')

    def upl_to_db(self, dataset, db_params):
        """
        Загружает данные в БД
        Параметры подключения 'postgresql://username:password@localhost:5432/mydatabase'
        """
        engine = create_engine(db_params)
        data = dataset.drop('id', axis=1)
        data.to_sql(name=self.data_table_name, con=engine, if_exists='append', index=False)
        print('Данные записаны в БД')

    def get_products_list(self):
        """
        Загружает из базы таблицу со списком продуктов
        """
        try:
            df = pd.read_sql(self.product_list_resp, psycopg2.connect(self.db_access))
            print('Загружена product_list')
            return df
        except:
            print('Доступ к таблице запрещен')

    @staticmethod
    def get_data_by_response(sql_resp, db_params):
        """
        Загружает таблицу по SQL-запросу
        """
        engine = create_engine(db_params)
        try:
            print('Загружается таблица')
            data = pd.read_sql(sql_resp, con=engine)
            print('Загружена таблица по SQL-запросу')
            return data
        except:
            print('Произошла непредвиденная ошибка')
            return None

    def get_analitics_data_head(self, db_params):
        """
        Загружает таблицу из базы
        """
        query = f"SELECT * FROM analitics_data2 LIMIT 100"
        print('Загружается analitics_data')
        engine = create_engine(db_params)
        self.db_data = pd.read_sql(query, con=engine)
        print('Загружено 100 строк analitics_data')

    def add_new_access_data(self,
                            ecom_client_id: int,
                            name: str,
                            client_id: str,
                            client_secret: str,
                            ozon_perf_mp_id=14,
                            ozon_perf_client_id_attribute_id=9,
                            ozon_perf_client_secret_attribute_id=8,
                            status='Active'):
        """
        Добавляет в базу новые данные для доступа к ozon performance для пользователя
        """
        add_to_acc_list_query = f"INSERT INTO account_list (mp_id, client_id, status_1, name) " \
                                f"VALUES ({ozon_perf_mp_id}, {ecom_client_id}, '{status}', '{name}')"

        id_query = f"SELECT id FROM account_list " \
                   f"WHERE client_id = {ecom_client_id} AND mp_id = {ozon_perf_mp_id} AND name = '{name}'"

        try:
            conn = psycopg2.connect(self.db_access)
            q = conn.cursor()
            q.execute(add_to_acc_list_query)
            conn.commit()
            status = q.statusmessage
            q.close()
            conn.close()
        except:
            print('Нет подключения к БД, или нет доступа на выполнение операции')
            return None

        if status is not None:
            try:
                conn = psycopg2.connect(self.db_access)
                q = conn.cursor()
                q.execute(id_query)
                result = q.fetchall()
                print(result)
                conn.close()
            except:
                print('Нет подключения к БД, или нет доступа на выполнение операции')
                return None

            if result is not None:
                id_ = result[0][0]

                add_to_acc_service_data_query = f"INSERT INTO account_service_data (account_id, attribute_id, " \
                                                f"attribute_value) " \
                                                f"VALUES ({id_}, {ozon_perf_client_id_attribute_id}, '{client_id}'), " \
                                                f"({id_}, {ozon_perf_client_secret_attribute_id}, '{client_secret}')"
                try:
                    conn = psycopg2.connect(self.db_access)
                    q = conn.cursor()
                    q.execute(add_to_acc_service_data_query)
                    conn.commit()
                    # status = q.statusmessage
                    q.close()
                    conn.close()
                    return 'OK'
                except:
                    print('Нет подключения к БД, или нет доступа на выполнение операции')
                    return None


