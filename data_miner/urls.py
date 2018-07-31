from django.conf.urls import include, url

urlpatterns = [

    # preprocessor
    url(r'^process/', include('preprocessor.urls')),

    # action_logger
    url(r'^log/', include('actionlogwriter.urls')),

    # data_ager
    url(r'^aging/', include('data_ager.urls')),

    # data_migrator
    url(r'^migration/', include('migrator.urls')),

]
