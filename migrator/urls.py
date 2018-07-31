from django.conf.urls import url

from migrator.views import migrator_views

urlpatterns = [

    # data_migrator
    url(r'^$', migrator_views.migrator_controller, name='migrator_controller'),

]