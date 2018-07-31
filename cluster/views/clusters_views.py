# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from cluster.models import *

# rest
from rest_framework.decorators import api_view
from rest_framework import status
from rest_framework.response import Response

# datetime
from datetime import datetime, timedelta
from django.utils import timezone

# tz
import pytz

# scikit-learn
from sklearn.cluster import AgglomerativeClustering

# pandas
import pandas as pd

# log
import sys
import traceback

# ordreddict
import collections

# itertools
import itertools

# threading
import threading

# excel writer
import xlsxwriter

# db aggregation tool
from django.db.models import F, Sum, Count, Case, When
from django.db.models import CharField, Value as V
from django.db.models.functions import Concat

# aws s3
import boto3

# os
import os

# copy
import copy

# global values
THE_NUMBER_OF_FEATURE = 1
THE_NUMBER_OF_CLUSTER_GROUP = 3
TIME_GAP = 9

GROUP_ALL = "ALL"
GROUP_SHORT = "SHORT"
GROUP_LONG = "LONG"
GROUP_MIX = "MIX"

SLACK_TOKEN = "YOUR_SLACK_TOKEN"
AWS_ACCESS_KEY_ID = "YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_KEY_ID"
ENDPOINT = "ap-northeast-2"

# global values
rearranged_user_dict = dict()
rearranged_user_character_dict = dict()

def get_deal_distribution():
    """
    최근 3개월간 딜 분포를 통해 상품 지역에 대한 가중치를 계산
    :return: deal_distribution_weight_dict = { category_id : weight }
    """

    # get dictionary of area -> area_dict = { category_id : [ arrival_ids] }
    TIME_GAP = 9
    DISTRIBUTION_STANDARAD = 90

    print('-get_deal_distribution START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    area_dict = dict ()
    category_list = [6, 7, 9, 11, 12, 13, 15, 16, 18]

    # get category vs area list
    for category_id in category_list:
        area_list = Area.objects.filter(category_id__exact=category_id) \
                                .values_list('id', flat=True) \
                                .order_by('id')
        area_list = list(area_list)
        area_dict[category_id] = area_list

    # get ticket count
    tickets = Ticket.objects.filter(created_at__gte=timezone.now() + timedelta(hours=TIME_GAP) - timedelta(days=DISTRIBUTION_STANDARAD),
                                    created_at__lte=timezone.now() + timedelta(hours=TIME_GAP)) \
                            .values('deal_id') \
                            .annotate(arrivals = GroupConcat('arrival_id', distinct=True, ordering='arrival_id ASC', separator=',')) \
                            .order_by('deal_id')
    ticket_object_list = list(tickets)

    # get deal_arrival_dict { deal_id : [arrival_ids] }
    deal_arrival_dict = dict()
    for ticket_object in ticket_object_list:
        deal_id = ticket_object["deal_id"]
        arrivals = ticket_object["arrivals"].split(',')
        deal_arrival_dict[deal_id] = arrivals

    # get deal_category_count { 'category_id' : x_count }
    deal_category_count = collections.Counter()

    for deal_id, arrival_id_list in deal_arrival_dict.iteritems():
        temp_deal_category_count_set = set()

        for arrival_id in arrival_id_list:
            for category_id, area_id_list in area_dict.iteritems():
                if int(arrival_id) in area_id_list:
                    temp_deal_category_count_set.add(str(category_id))

        deal_category_count.update(temp_deal_category_count_set)

    # get deal_distribution_weight_dict { 'category_id' : weight }
    deal_category_count = dict(deal_category_count)
    deal_distribution_weight_dict = {k : sum(deal_category_count.values(), 0.0) / v for k, v in deal_category_count.iteritems()}

    # reset weight in deal_distribution_weight_dict
    for k, v in deal_distribution_weight_dict.iteritems():
        deal_distribution_weight_dict[k] = 1

    # delete variable that are no use
    del deal_arrival_dict
    del ticket_object_list
    del area_dict
    del area_list

    print('-get_deal_distribution COMPLETE ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return deal_distribution_weight_dict


def get_whole_character_group_data():
    """
    전체 캐릭터 그룹의 데이터를 가져옴
    :return: temp_whole_big_cluster_group = {
                                                'ALL,CATEGORY6,THEME1' : [(442234, 0), (442235, 1), (442236, 2)]
                                                'description'          : [(character_group_id, name)] }
    """

    temp_whole_big_cluster_group = dict()

    try:
        character_groups = Character_group.objects.order_by('id', 'description').values()
        character_groups = list(character_groups)

    except Exception as e:
        print('- get_whole_character_group_data ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))
        return

    for character_group_object in character_groups:

        temp_character_group_object_list = []
        key = (character_group_object['id'], character_group_object['name'])
        # value = [Users]

        if character_group_object['description'] in temp_whole_big_cluster_group.keys():
            temp_character_group_object_list = temp_whole_big_cluster_group[character_group_object['description']]

        temp_character_group_object_list.append(key)
        temp_whole_big_cluster_group[character_group_object['description']] = temp_character_group_object_list

    print('- get_whole_character_group_data ' \
          + 'BIG CLUSTER GROUP : {} ,'.format(str(len(temp_whole_big_cluster_group.keys())))
          + 'CHARACTER GROUPS : {} '.format(str(len(list(itertools.chain.from_iterable(temp_whole_big_cluster_group.values())))))
          + ' load COMPLETE ' \
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return temp_whole_big_cluster_group


def cluster_agglomerative_clustering(description_vs_user_character_dict):
    """
    큰 클러스터그룹 내의 사용자를 받아 2차 클러스터링(상 / 중 / 하)를 구동
    :param description_vs_user_character_dict:
    :return: newly_clustered_group_dict = {
                                                description : {
                                                                    0 : [users]
                                                                    1 : [users]
                                                                    2 : [users]
                                                            }
                                            }
    """
    global rearranged_user_dict
    global rearranged_user_character_dict

    print('- cluster_agglomerative_clustering START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    description_ordered_group_list_dict = dict()

    for description, list_of_user_character in description_vs_user_character_dict.iteritems():

        temp_user_character_group_ordered = dict()
        user_id_list = []

        # check for over than number 3
        if (len(list_of_user_character) > 3):

            user_character_group_no_order = {}
            user_group_conv_per_people = {}

            ordered_group_dictionary = {}
            temp_list = []
            temp_group = []

            # change data into pandas
            for dictionary in list_of_user_character:
                # insert user id
                user_id_list.append(dictionary['user_id'])

                # change into pandas
                pandas_data = pd.DataFrame(dictionary, columns=[dictionary.keys()], index=[0])
                temp_list.append(pandas_data)

                # sum values
                del dictionary['user_id']

            # merge pandas
            merged_pandas_result = pd.concat(temp_list)
            del merged_pandas_result['user_id']

            try:
                # cluster users
                cluster = AgglomerativeClustering(n_clusters=THE_NUMBER_OF_CLUSTER_GROUP, affinity='euclidean',
                                                  linkage='ward')
                cluster_group_label_raw_list = cluster.fit_predict(merged_pandas_result.as_matrix())
                cluster_group_label_list = cluster_group_label_raw_list.reshape(-1, 1)
            except Exception as e:
                print('- cluster_agglomerative_clustering ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                print(''.join('* ' + line for line in lines))

            # make group by label
            for index in range(len(cluster_group_label_list)):

                if cluster_group_label_list[index][0] in user_character_group_no_order:
                    temp_group = user_character_group_no_order[cluster_group_label_list[index][0]]

                temp_group.append(user_id_list[index])
                user_character_group_no_order[cluster_group_label_list[index][0]] = temp_group
                temp_group = []

            # retrieve user count and conversion count in group, and match with label
            for key, user_list in user_character_group_no_order.iteritems():

                try:
                    user_count = len(user_list)
                    user_conv_count = Action_log.objects.filter(user_id__in=user_list).count()
                    user_group_conv_per_people[key] = (user_count, user_conv_count)

                except Exception as e:
                    print('- cluster_agglomerative_clustering ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    print(''.join('* ' + line for line in lines))

            # set order by conversion per people in group
            user_character_group_ordered_list = sorted(user_group_conv_per_people.iteritems(),
                                                       key=lambda user_tuple: (
                                                           (float(user_tuple[1][1]) / float(user_tuple[1][0])),
                                                           user_tuple[1][0]),
                                                       reverse=True)
            index = 0

            # re- labeling
            for key, value in user_character_group_ordered_list:
                ordered_group_dictionary[key] = index
                index = index + 1

            # make ordered key - users dict
            for key, value in user_character_group_no_order.iteritems():
                temp_user_character_group_ordered[ordered_group_dictionary[key]] = user_character_group_no_order[key]

            # make description - ordered key - users dict
            description_ordered_group_list_dict[description] = temp_user_character_group_ordered

        # if people in group are lower than 3 people than assign group and set name 0.
        else:
            for dictionary in list_of_user_character:
                user_id_list.append(dictionary['user_id'])

            temp_user_character_group_ordered[0] = user_id_list
            description_ordered_group_list_dict[description] = temp_user_character_group_ordered

    print('- cluster_agglomerative_clustering COMPLETE ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return description_ordered_group_list_dict


def rearrange_users(temp_sorted_user_list, temp_sorted_user_character_list):
    """
    각 캐릭터 그룹의 업데이트된 필터와 캐릭터를 가지고 전체 유저 및 캐릭터 글로벌 변수에 정보를 계속 업데이트
    :param temp_sorted_user_list:
    :param temp_sorted_user_character_list:
    :return:
    """
    print('- REARRANGE CLUSTER GROUP START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    global rearranged_user_dict
    global rearranged_user_character_dict

    for description, user_list in temp_sorted_user_list.iteritems():

        if description not in rearranged_user_dict.keys():
            rearranged_user_dict[description] = user_list
        else:
            rearranged_user_dict[description].extend(user_list)

    for description, user_character_list in temp_sorted_user_character_list.iteritems():

        if description not in rearranged_user_character_dict.keys():
            rearranged_user_character_dict[description] = user_character_list
        else:
            rearranged_user_character_dict[description].extend(user_character_list)

    print('- REARRANGE CLUSTER GROUP FINISHED ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))


def make_changed_description(temp_cluster_group_users, cluster_group_user_filters, cluster_group_user_characters):
    """
    새롭게 변경된 유저들의 필터정보와 캐릭터 정보들을 합하여, 변경된 description을 형성
    :param temp_cluster_group_users:
    :param cluster_group_user_filters:
    :param cluster_group_user_characters:
    :return:
    """
    print('- MAKE CHANGED CLUSTERING GROUP START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    changed_description_dict = {}
    temp_sorted_user_list = {}
    temp_sorted_user_character_list = {}

    # make list flatten
    user_list = list(itertools.chain.from_iterable(temp_cluster_group_users.values()))

    for user_id in user_list:

        temp_user_list = []
        temp_user_character_list = []

        try:
            changed_description = cluster_group_user_filters[user_id]

            for ordered_dict_key in cluster_group_user_characters[user_id].keys():
                if ordered_dict_key != 'user_id':
                    changed_description = changed_description + ',' + ordered_dict_key

            changed_description_dict[user_id] = changed_description

            if changed_description in temp_sorted_user_list.keys():
                temp_user_list = temp_sorted_user_list[changed_description]
                temp_user_character_list = temp_sorted_user_character_list[changed_description]

            temp_user_list.append(user_id)
            temp_user_character_list.append(cluster_group_user_characters[user_id])
            temp_sorted_user_list[changed_description] = temp_user_list
            temp_sorted_user_character_list[changed_description] = temp_user_character_list

        except Exception as e:
            continue

    print('- MAKE CHANGED CLUSTERING GROUP COMPLETED ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return (temp_sorted_user_list, temp_sorted_user_character_list)


def get_character_group_data(character_groups, deal_weight_dict):
    """
    전체 캐릭터 그룹 정보를 넘겨받아 각 그룹을 반복하면서 아래 종속된 메서드들을 실행
    :param character_groups:
    :return:
    """
    print('- UPDATE CLUSTER START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    for description, character_group_object_list in character_groups.iteritems():

        temp_character_group_list = []

        for character_group_id, name in character_group_object_list:
            temp_character_group_list.append(character_group_id)

        temp_cluster_group_users = {}

        try:
            temp_users_in_each_big_group = User_character.objects \
                .values_list('user_id', flat=True) \
                .filter(character_group_id__in=temp_character_group_list) \
                .exclude(is_closed=True) \
                .distinct() \
                .order_by('user_id')
            temp_users_in_each_big_group = list(temp_users_in_each_big_group)

        except Exception as e:
            print('- get_character_group_data ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))
            return

        print (len(temp_users_in_each_big_group))

        temp_cluster_group_users[description] = temp_users_in_each_big_group
        temp_cluster_group_user_filters, basic_group_user = get_user_filter(temp_cluster_group_users)
        temp_cluster_group_user_characters = get_user_character(temp_cluster_group_users, basic_group_user, deal_weight_dict)

        temp_sorted_user_list, temp_sorted_user_character_list = make_changed_description(temp_cluster_group_users,
                                                                                          temp_cluster_group_user_filters,
                                                                                          temp_cluster_group_user_characters)

        print (len(list(itertools.chain.from_iterable(temp_sorted_user_list.values()))))
        print (len(list(itertools.chain.from_iterable(temp_sorted_user_character_list.values()))))

        # update original dict
        rearrange_users(temp_sorted_user_list, temp_sorted_user_character_list)

    print('- UPDATE CLUSTER COMPLETE ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))


def get_user_filter(temp_cluster_group_users):
    """
    각 그룹에 속하는 유저들의 필터 설정값을 불러옴
    :param temp_cluster_group_users:
    :return:
    """
    global GROUP_ALL
    global GROUP_SHORT
    global GROUP_LONG
    global GROUP_MIX

    print('- MAKE USER-FILTER START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    all_checked_group = [u'6', u'7', u'9', u'11', u'12', u'13', u'15', u'16', u'18']
    short_distance_checked_group = [u'11', u'12', u'13', u'15', u'16']
    long_distance_checked_group = [u'6', u'7', u'9']
    temp_user_category_strings_dictionary = dict()

    users_to_call = list(itertools.chain.from_iterable(temp_cluster_group_users.values()))

    basic_group_user = None

    # if cluster group is basic group, call user in different way
    if 'BASIC GROUP' in temp_cluster_group_users.keys():

        try:
            print('- BASIC GROUP MAKE USER-FILTER START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            user_who_has_user_character_category = Character_category.objects.raw("SELECT DISTINCT character_category.user_id, character_category.id FROM character_category WHERE character_category.user_id IN(SELECT DISTINCT user_character.user_id FROM user_character WHERE user_character.user_id IN (SELECT user.id FROM user WHERE NOT user.user_type_id=4 AND user.category_strings IS NOT NULL ) AND user_character.is_closed=0 AND user_character.character_group_id=1)")
            user_who_has_user_character_category = set(user_who_has_user_character_category)

            user_who_has_user_character_theme = Character_category.objects.raw("SELECT DISTINCT user_id, id FROM character_theme WHERE user_id IN (SELECT DISTINCT user_id FROM user_character WHERE user_id IN (SELECT id FROM user WHERE NOT user_type_id=4 AND category_strings IS NOT NULL ) AND is_closed=0 AND character_group_id=1)")
            user_who_has_user_character_theme = set(user_who_has_user_character_theme)

            users_to_call = set()

            for object in itertools.chain(user_who_has_user_character_category, user_who_has_user_character_theme):

                users_to_call.add(object.user_id)

            print('- get_user_filter ' +  'character ' + str(len(user_who_has_user_character_category))
                  + ' theme ' + str(len(user_who_has_user_character_theme)) + ' load COMPLETE '
                  + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            print('- get_user_filter ' +  'user ' + str(len(users_to_call))
                  + ' load COMPLETE ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

            basic_group_user = users_to_call

            user_category_strings = User.objects.filter(id__in=users_to_call) \
                                                                        .values_list('id', 'category_strings')
            user_category_strings = list(user_category_strings)

            print('- get_user_filter ' + 'user_category_strings ' + str(len(user_category_strings))
                  + ' load COMPLETE ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

        except Exception as e:
            print('- get_user_filter ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))
            return
    else:
        try:
            user_category_strings = User.objects.filter(id__in=users_to_call) \
                                                .filter(category_strings__isnull=False) \
                                                .exclude(user_type_id__exact=4) \
                                                .values_list('id', 'category_strings')
            user_category_strings = list(user_category_strings)
            print(len(user_category_strings))
        except Exception as e:

            print('- get_user_filter ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))
            return

    for user_object in user_category_strings:

        try:
            category_list = user_object[1].split(",")
            category_list = category_list[:-1]

        except Exception as e:
            # except user whose category_strings is null
            continue

        if len(category_list) != 0:
            # all_checked_group
            if len(category_list) == len(all_checked_group):
                temp_user_category_strings_dictionary[user_object[0]] = GROUP_ALL

            # short_distance_checked_group
            elif set(category_list).issubset(short_distance_checked_group):
                temp_user_category_strings_dictionary[user_object[0]] = GROUP_SHORT

            # long_distance_checked_group
            elif set(category_list).issubset(long_distance_checked_group):
                temp_user_category_strings_dictionary[user_object[0]] = GROUP_LONG

            # mixed_checked_group
            else:
                temp_user_category_strings_dictionary[user_object[0]] = GROUP_MIX
        # add user whose category_strings is null
        else:
            pass

    print('- MAKE USER-FILTER DONE ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return (temp_user_category_strings_dictionary, basic_group_user)


def get_user_character(temp_cluster_group_users, basic_group_user, deal_weight_dict):
    """
    각 그룹에 속하는 사용자들의 유저 캐릭터를 불러와 카테고리 1순위, 테마 1순위로 추려냄
    :param temp_cluster_group_users:
    :param basic_group_user:
    :return:
    """

    print('- MAKE USER-CHARACTER START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    inserted_user_list = []
    temp_user_searched_character_category = dict()
    temp_user_searched_character_theme = dict()
    temp_user_searched_character = dict()

    users_to_call = list(itertools.chain.from_iterable(temp_cluster_group_users.values()))

    # if cluster group is basic group, call user in different way
    if 'BASIC GROUP' in temp_cluster_group_users.keys():

        print('- BASIC GROUP MAKE USER-CHARACTER START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

        try:
            users_to_call = basic_group_user

            temp_user_character_categories = Character_category.objects \
                .filter(user_id__in=users_to_call) \
                .values('user_character_id', 'user_id', 'category') \
                .annotate(score=Sum('score'), cnt_rcd=Count('user_character_id')) \
                .order_by('user_character_id', '-score', '-category')
            temp_user_character_categories = list(temp_user_character_categories)

            temp_user_character_themes = Character_theme.objects \
                .filter(user_id__in=users_to_call) \
                .values('user_character_id', 'user_id', 'theme') \
                .annotate(score=Sum('score'), cnt_rcd=Count('user_character_id')) \
                .order_by('user_character_id', '-score', '-theme')
            temp_user_character_themes = list(temp_user_character_themes)

        except Exception as e:
            print(
            '- get_user_character' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))
            return
    else:
        try:

            temp_user_character_categories = Character_category.objects \
                .filter(user_id__in=users_to_call) \
                .exclude(is_closed=True) \
                .values('user_character_id', 'user_id', 'category') \
                .annotate(score=Sum('score'), cnt_rcd=Count('user_character_id')) \
                .order_by('user_character_id', '-score', '-category')
            temp_user_character_categories = list(temp_user_character_categories)

            temp_user_character_themes = Character_theme.objects \
                .filter(user_id__in=users_to_call) \
                .exclude(is_closed=True) \
                .values('user_character_id', 'user_id', 'theme') \
                .annotate(score=Sum('score'), cnt_rcd=Count('user_character_id')) \
                .order_by('user_character_id', '-score', '-theme')
            temp_user_character_themes = list(temp_user_character_themes)

        except Exception as e:
            print('- get_user_character ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))
            return

    # fetch all user's character point and make a dict { 'user_id' : { 'category_id' : point } }
    user_character_point = {}
    for user_category_character in temp_user_character_categories:

        if (user_category_character['user_id'] not in inserted_user_list):
            inserted_user_list.append(user_category_character['user_id'])
            temp_user_character_point={}
            temp_user_character_point[str(user_category_character['user_id'])] = {}

        temp_user_character_point[str(user_category_character['user_id'])][str(user_category_character['category'])] = user_category_character['score']
        user_character_point.update(temp_user_character_point)

    # get adjust point multiplied by deal_distribution_weight_dict and after that extract 1st category value and its score
    for user_id, point_dict in user_character_point.iteritems():

        updated_user_character_point = {k : int(round(v * deal_weight_dict[k])) for k, v in point_dict.items() if k in deal_weight_dict}
        sorted_updated_user_character_point = sorted(updated_user_character_point.iteritems(), key=lambda x: x[1], reverse=True)

        temp_category_dictionary = collections.OrderedDict()
        temp_category_dictionary['user_id'] = int(user_id)

        try:
            category_id, point = sorted_updated_user_character_point[0]
            temp_category_dictionary["CATEGORY" + category_id] = point

        except Exception as e:
            temp_category_dictionary["CATEGORY_NONE1"] = 0

        # make temp_user_searched_character_category dict { user_id : OrderedDict([('user_id', user_id), ('CATEGORYX', point)]) }
        if (int(user_id) not in temp_user_searched_character_category.keys()):
            temp_user_searched_character_category[int(user_id)] = temp_category_dictionary

    # extract 1st theme value and its score
    for user_theme_character in temp_user_character_themes:

        if (user_theme_character['user_id'] not in inserted_user_list):
            inserted_user_list.append(user_theme_character['user_id'])

        temp_theme_dictionary = collections.OrderedDict()
        temp_theme_dictionary['user_id'] = user_theme_character['user_id']
        temp_theme_dictionary["THEME" + str(user_theme_character['theme'])] = user_theme_character['score']

        if (user_theme_character['user_id'] not in temp_user_searched_character_theme.keys()):
            temp_user_searched_character_theme[user_theme_character['user_id']] = temp_theme_dictionary

    # merge category_character and theme_character into user_character
    for user_id in set(inserted_user_list):

        result = collections.OrderedDict()

        if (user_id in temp_user_searched_character_category.keys()) and (
            user_id in temp_user_searched_character_theme.keys()):

            result.update(temp_user_searched_character_category[user_id])
            result.update(temp_user_searched_character_theme[user_id])
            temp_user_searched_character[user_id] = result

        elif (user_id in temp_user_searched_character_category.keys()) and (
            user_id not in temp_user_searched_character_theme.keys()):

            result.update(temp_user_searched_character_category[user_id])
            result['THEME_NONE1'] = 0
            temp_user_searched_character[user_id] = result

        elif (user_id not in temp_user_searched_character_category.keys()) and (
            user_id in temp_user_searched_character_theme.keys()):

            result['user_id'] = user_id
            result['CATEGORY_NONE1'] = 0
            result.update(temp_user_searched_character_theme[user_id])
            temp_user_searched_character[user_id] = result
        else:
            pass
    print('- MAKE USER-CHARACTER DONE ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return temp_user_searched_character


def save_updated_character_group(character_groups, newly_clustered_group_dict, to_be_deleted_group_list):
    """
    새롭게 클러스터링 된 그룹을 저장하는 메서드
    :param character_groups:
    :param newly_clustered_group_dict:
    :param to_be_deleted_group_list:
    :return:
    """
    print('- SAVE NEW CLUSTERING GROUP START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    # create_character_group
    for description, second_clustered_dict in newly_clustered_group_dict.iteritems():

        # if new character group exists
        if description not in character_groups.keys():

            character_group_list = []

            # create_character_group_object
            for name in sorted(second_clustered_dict.keys()):

                if name == 0:
                    character_group = Character_group(created_at=timezone.now() + timedelta(hours=TIME_GAP),
                                                      last_modified=timezone.now() + timedelta(hours=TIME_GAP),
                                                      name=str(name), description=description,
                                                      represent_character_id=None, parent_group_id=None)
                else:
                    character_group = Character_group(created_at=timezone.now() + timedelta(hours=TIME_GAP),
                                                      last_modified=timezone.now() + timedelta(hours=TIME_GAP),
                                                      name=str(name), description=description,
                                                      represent_character_id=None,
                                                      parent_group_id=character_group_list[0]['id'])

                Character_group.save(character_group)
                character_group_list.append(character_group)

            # update user character group
            for name, user_list in second_clustered_dict.iteritems():

                # save represent character
                try:
                    setattr(character_group_list[name], 'represent_character_id', user_list[0])
                    Character_group.save(character_group_list[name])
                except Exception as e:
                    print('- update_Character_group_objects_with_represent_character_id ' + str(
                        datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    print(''.join('* ' + line for line in lines))

                # update user character
                try:
                    User_character.objects.filter(user_id__in=user_list).update(
                        character_group_id=character_group_list[name]['id'],
                        last_modified=timezone.now() + timedelta(hours=TIME_GAP))
                except Exception as e:
                    print('- update_character_groups ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    print(''.join('* ' + line for line in lines))

        # if cluster group is in existing cluster group
        else:
            for name, user_list in sorted(second_clustered_dict.iteritems(), key=lambda x: x[0]):
                # if no changes in character group, then just update represent users and user character
                if len(character_groups[description]) == len(second_clustered_dict.keys()):
                    try:
                        character_group_id = character_groups[description][name][0]
                        Character_group.objects.filter(id=character_group_id)\
                                                .update(represent_character_id=user_list[0],
                                                        last_modified=timezone.now() + timedelta(hours=TIME_GAP))
                        User_character.objects.filter(user_id__in=user_list)\
                                                .update(character_group_id=character_group_id,
                                                        last_modified=timezone.now() + timedelta(hours=TIME_GAP))
                    except Exception as e:
                        pass

                # if existing group is bigger than modified group
                elif len(character_groups[description]) > len(second_clustered_dict.keys()):

                    if name == 0:
                        character_group_id = character_groups[description][name][0]
                        try:
                            Character_group.objects.filter(id=character_group_id)\
                                                    .update(represent_character_id=user_list[0],
                                                            last_modified=timezone.now() + timedelta(hours=TIME_GAP))
                            User_character.objects.filter(user_id__in=user_list)\
                                                    .update(character_group_id=character_group_id,
                                                            last_modified=timezone.now() + timedelta(hours=TIME_GAP))
                        except Exception as e:
                            pass

                    # filter other group don't exists any more.
                    else:
                        for id_num, name_id in character_groups[description]:
                            if id_num != character_group_id:
                                # add character_group_id to be deleted character group
                                to_be_deleted_group_list.append(id_num)

                # if modified group is bigger than original group
                elif len(character_groups[description]) < len(second_clustered_dict.keys()):

                    # save parent group id in case of new character group
                    parent_group_id = character_groups[description][name][0]

                    # update existing group and user
                    if name == 0:
                        try:
                            Character_group.objects.filter(id=character_group_id)\
                                                    .update(represent_character_id=user_list[0],
                                                            last_modified=timezone.now() + timedelta(hours=TIME_GAP))
                            User_character.objects.filter(user_id__in=user_list)\
                                                    .update(character_group_id=parent_group_id,
                                                            last_modified=timezone.now() + timedelta(hours=TIME_GAP))
                        except Exception as e:
                            pass

                    # create new group and save it
                    else:
                        try:
                            character_group = Character_group(created_at=timezone.now() + timedelta(hours=TIME_GAP),
                                                              last_modified=timezone.now() + timedelta(hours=TIME_GAP),
                                                              name=str(name), description=description,
                                                              represent_character_id=user_list[0],
                                                              parent_group_id=parent_group_id)
                            Character_group.save(character_group)
                            User_character.objects.filter(user_id__in=user_list)\
                                                    .update(character_group_id=character_group['id'],
                                                            last_modified=timezone.now() + timedelta(hours=TIME_GAP))
                        except Exception as e:
                            pass

    print('- SAVE NEW CLUSTERING GROUP START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))


def write_report_to_excel(final_data_list):
    """
    전달받은 데이터를 이용하여 클러스터링 보고서를 작성
    :param final_data_list:
    :return:
    """
    print('- write_report_to_excel START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    if len(final_data_list) > 0:

        time_dt = timezone.now() + timedelta(hours=TIME_GAP)
        time_string = time_dt.strftime("%Y%m%d%H")

        workbook = xlsxwriter.Workbook("cluster_report_" + time_string + ".xlsx")
        bold = workbook.add_format({'bold': True})
        worksheet_1 = workbook.add_worksheet("character_group_report_raw_data")

        worksheet_1.write('A1', 'character_group_id', bold)
        worksheet_1.write('B1', 'description', bold)
        worksheet_1.write('C1', 'sum_people', bold)
        worksheet_1.write('D1', 'conversion_count_sum', bold)
        worksheet_1.write('E1', 'parent_group_id', bold)
        worksheet_1.write('F1', 'id', bold)
        worksheet_1.write('G1', 'name', bold)
        worksheet_1.write('H1', 'deal_id', bold)
        worksheet_1.write('I1', 'conversion_count', bold)

        row = 1
        col = 0

        for character_group_id, group_info_dict in final_data_list.iteritems():

            if len(group_info_dict["conversion_count_list"]) == 0:
                worksheet_1.write(row, col, character_group_id)
                worksheet_1.write(row, col + 1, group_info_dict["description"])
                worksheet_1.write(row, col + 2, group_info_dict["sum_people"])
                worksheet_1.write(row, col + 3, group_info_dict["conversion_count_sum"])
                worksheet_1.write(row, col + 4, group_info_dict["parent_group_id"])
                worksheet_1.write(row, col + 5, group_info_dict["id"])
                worksheet_1.write(row, col + 6, group_info_dict["name"])
                worksheet_1.write(row, col + 7, 0)
                worksheet_1.write(row, col + 8, 0)
                row += 1

            for i in xrange(len(group_info_dict["conversion_count_list"])):
                worksheet_1.write(row, col, character_group_id)
                worksheet_1.write(row, col + 1, group_info_dict["description"])
                worksheet_1.write(row, col + 2, group_info_dict["sum_people"])
                worksheet_1.write(row, col + 3, group_info_dict["conversion_count_sum"])
                worksheet_1.write(row, col + 4, group_info_dict["parent_group_id"])
                worksheet_1.write(row, col + 5, group_info_dict["id"])
                worksheet_1.write(row, col + 6, group_info_dict["name"])
                worksheet_1.write(row, col + 7, group_info_dict["conversion_count_list"][i]["type_id"])
                worksheet_1.write(row, col + 8, group_info_dict["conversion_count_list"][i]["count"])
                row += 1

        workbook.close()

        # upload a excel file to s3
        s3 = boto3.client('s3', region_name=ENDPOINT,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        file_path = "cluster_report_" + time_string + ".xlsx"
        path = "temp/cluster_report_daily/"
        bucket = "playwings-log-bucket"
        result_url = path + file_path

        s3.upload_file(file_path, bucket, result_url)

        print('- CLUSTER INFO UPLOAD COMPLETED AT {} S3 '.format(result_url) + str(
            datetime.now(tz=pytz.timezone('Asia/Seoul'))))

        os.remove(file_path)

    else:
        print ('- NO DATA TO WRITE ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    print('- write_report_to_excel COMPLETED ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))


def get_clustering_report_data():
    """
    클러스터링 보고서 작성을 위한 데이터를 불러옴
    :return:
    """
    print('- get_clustering_report_data START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    # get each group's data
    try:
        character_group_ids = Character_group.objects.all().values_list('id', flat=True).exclude(id=1)
        character_group_ids = list(character_group_ids)
    except Exception as e:
        print('- get_clustering_report_data ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))

    result_dic = {}

    for id in character_group_ids:

        temp_dic = {}
        character_group = Character_group.objects.filter(id=id).values()[0]
        temp_dic['id'] = id
        temp_dic['name'] = character_group['name']
        temp_dic['description'] = character_group['description']
        temp_dic['parent_group_id'] = character_group['parent_group_id']

        try:
            user_character_count = User_character.objects.filter(character_group_id=id).distinct().count()
            temp_dic['sum_people'] = user_character_count
        except Exception as e:
            print(
            '- get_clustering_report_data user_character_count' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))

        try:
            user_character_list = User_character.objects.filter(character_group_id=id) \
                                                        .values_list('user_id',flat=True) \
                                                        .exclude(id=1).distinct()

            user_character_list = list(user_character_list)

            action_log_count = Action_log.objects.filter(user_id__in=user_character_list).distinct().count()

            temp_dic['conversion_count_sum'] = action_log_count

            user_character_list = Action_log.objects.filter(user_id__in=user_character_list) \
                                                    .values('type_id') \
                                                    .annotate(count=Count('type_id')) \
                                                    .order_by('-count')

            temp_dic['conversion_count_list'] = list(user_character_list)
        except Exception as e:
            print('- get_clustering_report_data user_character_count' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))

        result_dic[str(id)] = copy.deepcopy(temp_dic)

    print('- get_clustering_report_data COMPLETED ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return result_dic


# main code
def get_clustering():

    """
    각 메서드들을 호출하는 메인 함수
    :return: 없음
    """

    global rearranged_user_dict
    global rearranged_user_character_dict

    print('- get_character_group_list START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    # get deal_distribution data
    deal_distribution_weight_dict = get_deal_distribution()

    # load original character group info
    character_groups = get_whole_character_group_data()

    # make updated cluster group with deal_distribution data
    get_character_group_data(character_groups, deal_distribution_weight_dict)

    # copy global variables for use
    description_vs_user_character_dict = copy.deepcopy(rearranged_user_character_dict)

    # make updated second cluster group
    newly_clustered_group_dict = cluster_agglomerative_clustering(description_vs_user_character_dict)

    # make character group list that to be deleted
    to_be_deleted_group = set(character_groups.keys()).difference(newly_clustered_group_dict.keys())
    to_be_deleted_group_list = []

    for description in to_be_deleted_group:
        for character_group_id, name in character_groups[description]:
            if character_group_id != 1:
                to_be_deleted_group_list.append(character_group_id)

    # update all user to character group id 1 before save(for user that has closed user_character)
    try:
        User_character.objects.all().update(character_group_id=1, last_modified= timezone.now() + timedelta(hours=TIME_GAP))
    except Exception as e:
        print('- get_clustering temp update character group ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))

    # save updated information to character_group
    save_updated_character_group(character_groups, newly_clustered_group_dict, to_be_deleted_group_list)

    # delete useless character_group
    try:
        Character_group.objects.filter(id__in=to_be_deleted_group_list).delete()
    except Exception as e:
        print('- get_clustering delete useless group ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))

    # make report
    final_data_list = get_clustering_report_data()
    write_report_to_excel(final_data_list)

    print('- CLUSTER COMPLETE ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))


from django.db.models import Aggregate, CharField


class GroupConcat(Aggregate):
    function = 'GROUP_CONCAT'
    template = '%(function)s(%(distinct)s%(expressions)s%(ordering)s%(separator)s)'

    def __init__(self, expression, distinct=False, ordering=None, separator=',', **extra):
        super(GroupConcat, self).__init__(
            expression,
            distinct='DISTINCT ' if distinct else '',
            ordering=' ORDER BY %s' % ordering if ordering is not None else '',
            separator=' SEPARATOR "%s"' % separator,
            output_field=CharField(),
            **extra
        )


@api_view(['POST'])
def clusters_controller(request):
    """
    클러스터링 뷰를 실행시키는 컨트롤러
    :param request: request type (post)
    :return: http response
    """
    if request.method == 'POST':

        try:
            # start thread
            thread = ClusterThread()
            thread.setDaemon(True)
            thread.start()

            return Response("clustering success", status=status.HTTP_200_OK)

        except Exception as e:
            print('- clusters_controller POST error ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))
            return Response("clustering fail", status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ClusterThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        get_clustering()
