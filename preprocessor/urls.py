from django.conf.urls import url

from preprocessor.views import preprocess_views

urlpatterns = [

    # pre_process
    url(r'^$', preprocess_views.preprocess_controller, name='preprocess_controller'),

]