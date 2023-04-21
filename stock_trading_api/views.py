import json
from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, generics
from rest_framework import permissions
# from .models import Stock
# from .serializers import StockSerializer


class StockRecommendationsView(APIView):
    '''
    For this API, we really only need GET requests.
    This is so Mary can retrieve the daily stock recommendations.
    The API backend here will be making a request to Supabase to get the data.
    We will assume the data is already there thanks to the scheduled task in background_tasks.py.
    '''

    def get(self, request, *args, **kwargs):
        '''
        Returns the daily stock recommendations from Supabase.
        If the date of the recommendation entry is not today, return an error.
        '''
        return Response(status=status.HTTP_200_OK)

