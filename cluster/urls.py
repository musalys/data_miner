from django.conf.urls import url

from cluster.views import clusters_views

urlpatterns = [

    # cluster
    url(r'^$', clusters_views.clusters_controller, name='clusters_controller'),

]