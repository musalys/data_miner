from django.conf.urls import url

from data_ager.views import aging_views

urlpatterns = [

    # data_ager
    url(r'^$', aging_views.aging_controller, name='aging_controller'),

]