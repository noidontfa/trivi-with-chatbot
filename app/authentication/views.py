from django.shortcuts import render
from rest_framework import permissions, status
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.views import APIView
from django.utils.crypto import get_random_string
from django.contrib.auth.hashers import make_password, check_password

from .models import Organization, User
from .serializers import UserSerializer, OrganizationSerializer

import json

from django.db import connection

# Create your views here.
@api_view(['GET'])
def get_user_info(request):
    try: 
        user_serializer = UserSerializer(request.user)
        email = user_serializer.data['email']
        org_id = user_serializer.data['org_id']
        org_obj = Organization.objects.get(org_id=org_id)
        org_serializer = OrganizationSerializer(org_obj)
        org_name = org_serializer.data['org_name']


        return Response({
            'status': 200,
            'message': 'Get user info successfully',
            'username': email,
            'org_name': org_name,
        })
    except Exception as error:
        return Response({
            'status': 201,
            'message': error
        })

@api_view(['GET'])
def get_org_info(request):
    try: 
        user_serializer = UserSerializer(request.user)
        org_id = user_serializer.data['org_id']
        org_obj = Organization.objects.get(org_id=org_id)
        org_serializer = OrganizationSerializer(org_obj)
        org_name = org_serializer.data['org_name']
        org_key = org_serializer.data['org_secret_key']
        org_descript = org_serializer.data['org_description']


        return Response({
            'status': 200,
            'message': 'Get org info successfully',
            'org_name': org_name,
            'org_key': org_key,
            'org_descript': org_descript
        })
    except Exception as error:
        return Response({
            'status': 201,
            'message': error
        })

@api_view(['POST'])
def change_org_info(request):
    try: 
        user_serializer = UserSerializer(request.user)
        org_id = user_serializer.data['org_id']

        body = json.loads(request.body)
        org_name = body['orgName']
        org_descript = body['orgDescript']

        Organization.objects.filter(org_id=org_id).update(org_name=org_name, org_description=org_descript)

        return Response({
            'status': 200,
            'message': 'Change info successfully',
            'org_name': org_name,
            'org_descript': org_descript
        })
    except Exception as error:
        return Response({
            'status': 201,
            'message': error
        })
    
@api_view(['POST'])
def generate_secret_key(request):
    try: 
        user_serializer = UserSerializer(request.user)
        org_id = user_serializer.data['org_id']

        org_key_encoded = get_random_string()

        Organization.objects.filter(org_id=org_id).update(org_secret_key=org_key_encoded)

        return Response({
            'status': 200,
            'message': 'Generate new key successfully',
            'org_key': org_key_encoded,
        })
    except Exception as error:
        return Response({
            'status': 201,
            'message': error
        })


@api_view(['POST'])
def change_password(request):
    try: 
        status = 201
        message = ''

        user_serializer = UserSerializer(request.user)
        password = user_serializer.data['password']
        user_id = user_serializer.data['id']

        body = json.loads(request.body)
        old_password = body['oldPassword']
        new_password = body['newPassword']
        confirmed_password = body['confirmedPassword']

        if (not check_password(old_password, password)):
            message = 'You provided wrong old password'
        elif (new_password != confirmed_password):
            message = 'Confirmed password must match with new password'
        else:
            status = 200
            encrypted_new_password = make_password(confirmed_password)
            User.objects.filter(pk=user_id).update(password=encrypted_new_password)
            message = 'Change password successfully'


        return Response({
            'status': status,
            'message': message,
        })
    except Exception as error:
        return Response({
            'status': 201,
            'message': error
        })


@api_view(['POST'])
@authentication_classes([])
@permission_classes([])
def sign_up(request):
    try: 
        body = json.loads(request.body)
        org_name = body['org_name']
        org_descript = body['org_descript']
        org_key_encoded = get_random_string()

        email = body['email']
        password = body['password']

        try:
            user = User.objects.get(email=email)
            return Response({
                'status': 201,
                'message': 'This email existed',
            })

        except User.DoesNotExist:
            organization = Organization(org_name=org_name, 
                                        org_description=org_descript,
                                        org_secret_key=org_key_encoded)
            organization.save()

            org_id =  organization.org_id
            encrypted_password = make_password(password)
            user = User(email=email, password=encrypted_password, org_id=org_id)
            user.save()

            # create view table
            query = f"""
            CREATE VIEW view_{org_id}_data_customer AS
            SELECT data_customer.id,
                   data_customer.cus_id::integer  AS customer_id,
                   data_customer.cus_first_name   AS customer_first_name,
                   data_customer.cus_last_name    AS customer_last_name,
                   data_customer.cus_email        AS customer_email,
                   data_customer.cus_dob          AS customer_dob,
                   data_customer.cus_phone_num    AS customer_phone_num,
                   data_customer.cus_gender       AS customer_gender,
                   data_customer.cus_job_title    AS customer_job_title,
                   data_customer.cus_location     AS customer_location,
                   data_customer.cus_account_date AS customer_account_date
            FROM data_customer
            WHERE data_customer.inf_org_id = {org_id}::text;
            
            CREATE VIEW view_{org_id}_data_event AS
            SELECT
                data_event.id,
                data_event.ev_id::integer AS event_id,
                data_event.ev_type AS event_type,
                data_event.ev_cus_id::integer AS event_customer_id,
                data_event.ev_touchpoint_type AS event_touchpoint_type,
                data_event.ev_peusdo_user AS event_peusdo_user,
                data_event.ev_dev_category AS event_dev_category,
                data_event.ev_dev_brand AS event_dev_brand,
                data_event.ev_dev_os AS event_dev_os,
                data_event.ev_dev_browser AS event_dev_browser,
                data_event.ev_dev_language AS event_dev_language,
                data_event.ev_geo_continent AS event_geo_continent,
                data_event.ev_geo_sub_continent AS event_geo_sub_continent,
                data_event.ev_geo_country AS event_geo_country,
                data_event.ev_geo_city AS event_geo_city,
                data_event.ev_session_id AS event_session_id,
                data_event.ev_page_title AS event_page_title,
                data_event.ev_page_url AS event_page_url,
                data_event.ev_traffic_source AS event_traffic_source,
                data_event.ev_ip_address AS event_ip_address,
                data_event.ev_keyword AS event_keyword,
                data_event.ev_start_time AS event_start_time,
                data_event.ev_end_time AS event_end_time,
                data_event.ev_is_like AS event_is_like,
                data_event.ev_rate AS event_rate,
                data_event.ev_review AS event_review
            FROM
                data_event
            WHERE
                data_event.inf_org_id = {org_id}::text;
            
            
            CREATE VIEW view_{org_id}_data_event_product as
            SELECT data_event_item.id as id,
                   data_event_item.event_id::integer as event_id,
                   data_event_item.item_id::integer as item_id,
                   data_event_item.evi_description as event_product_description,
                   data_event_item.evi_extra_value_1::integer as event_product_extra_value_1,
                   data_event_item.evi_extra_value_2::integer as event_product_extra_value_2,
                   data_event_item.evi_extra_value_3::integer as event_product_extra_value_3
            FROM data_event_item
            WHERE data_event_item.inf_org_id = {org_id}::text;

            CREATE VIEW view_{org_id}_data_product as
            SELECT
                data_product.id,
                data_product.prod_id::integer AS product_id,
                data_product.prod_name AS product_name,
                data_product.prod_url AS product_url,
                data_product.prod_description AS product_description,
                data_product.prod_category_1 AS product_category_1,
                data_product.prod_category_2 AS product_category_2,
                data_product.prod_category_3 AS product_category_3,
                data_product.prod_quantity::integer AS product_quantity,
                data_product.prod_price::integer AS product_price,
                data_product.prod_from_date AS product_from_date,
                data_product.prod_to_date AS product_to_date
            FROM
                data_product
            WHERE
                data_product.inf_org_id = {org_id}::text;
                
            CREATE VIEW view_{org_id}_data_transaction AS
            SELECT
                data_transaction.id,
                data_transaction.trans_id::integer AS transaction_id,
                data_transaction.trans_cus_id::integer AS transaction_customer_id,
                data_transaction.trans_peusdo_user::integer AS transaction_peusdo_user,
                data_transaction.trans_revenue_value::integer AS transaction_revenue_value,
                data_transaction.trans_tax_value::integer AS transaction_tax_value,
                data_transaction.trans_refund_value::integer AS transaction_refund_value,
                data_transaction.trans_shipping_value::integer AS transaction_shipping_value,
                data_transaction.trans_shipping_type AS transaction_shipping_type,
                data_transaction.trans_shipping_address AS transaction_shipping_address,
                data_transaction.trans_status AS transaction_status,
                data_transaction.trans_time AS transaction_time
            FROM
                data_transaction
            WHERE
                data_transaction.inf_org_id = {org_id}::text;
        
            create view view_{org_id}_data_transaction_product as
            SELECT data_transaction_item.id,
                   data_transaction_item.trans_id as transaction_id,
                   data_transaction_item.item_id as product_id,
                   data_transaction_item.ti_quantity::integer as transaction_product_quantity,
                   data_transaction_item.ti_description as transaction_product_description,
                   data_transaction_item.ti_extra_value_1::integer as transaction_product_extra_value_1,
                   data_transaction_item.ti_extra_value_2::integer as transaction_product_extra_value_2,
                   data_transaction_item.ti_extra_value_3::integer as transaction_product_extra_value_3
            FROM data_transaction_item
            WHERE data_transaction_item.inf_org_id = {org_id}::text;
            """

            with connection.cursor() as cursor:
                cursor.execute(query)
                # cursor.fetchall()

            return Response({
                'status': 200,
                'message': 'Create account successfully',
            })

    except Exception as error:
        return Response({
            'status': 201,
            'message': error
        })
