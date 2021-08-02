# Copyright 2010-2012 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from oslo_utils.importutils import import_class

from trove.common import cfg
from trove.common import exception
from trove.common.strategies.cluster import strategy

from cinderclient.v3 import client as CinderClient
import glanceclient
from keystoneauth1.identity import v3
from keystoneauth1 import session as ka_session
from keystoneclient.service_catalog import ServiceCatalog
from neutronclient.v2_0 import client as NeutronClient
from novaclient.client import Client
from swiftclient.client import Connection

CONF = cfg.CONF


def normalize_url(url):
    """Adds trailing slash if necessary."""
    if not url.endswith('/'):
        return '%(url)s/' % {'url': url}
    else:
        return url


def get_endpoint(service_catalog, service_type=None,
                 endpoint_region=None, endpoint_type='publicURL'):
    """
    Select an endpoint from the service catalog

    We search the full service catalog for services
    matching both type and region. The client is expected to
    supply the region matching the service_type. There must
    be one -- and only one -- successful match in the catalog,
    otherwise we will raise an exception.

    Some parts copied from glance/common/auth.py.
    """
    endpoint_region = endpoint_region or CONF.service_credentials.region_name

    if not service_catalog:
        raise exception.EmptyCatalog()

    # per IRC chat, X-Service-Catalog will be a v2 catalog regardless of token
    # format; see https://bugs.launchpad.net/python-keystoneclient/+bug/1302970
    # 'token' key necessary to get past factory validation
    sc = ServiceCatalog.factory({'token': None,
                                 'serviceCatalog': service_catalog})
    urls = sc.get_urls(service_type=service_type, region_name=endpoint_region,
                       endpoint_type=endpoint_type)

    if not urls:
        raise exception.NoServiceEndpoint(service_type=service_type,
                                          endpoint_region=endpoint_region,
                                          endpoint_type=endpoint_type)

    return urls[0]


def dns_client(context):
    from trove.dns.manager import DnsManager
    return DnsManager()


def guest_client(context, id, manager=None):
    from trove.guestagent.api import API
    if manager:
        clazz = strategy.load_guestagent_strategy(manager).guest_client_class
    else:
        clazz = API
    return clazz(context, id)


def nova_client(context, region_name=None, password=None):
    if CONF.nova_compute_url:
        url = '%(nova_url)s%(tenant)s' % {
            'nova_url': normalize_url(CONF.nova_compute_url),
            'tenant': context.project_id}
    else:
        region = region_name or CONF.service_credentials.region_name
        url = get_endpoint(
            context.service_catalog,
            service_type=CONF.nova_compute_service_type,
            endpoint_region=region,
            endpoint_type=CONF.nova_compute_endpoint_type
        )

    client = Client(CONF.nova_client_version,
                    username=context.user,
                    password=password,
                    endpoint_override=url,
                    project_id=context.project_id,
                    project_domain_name=context.project_domain_name,
                    user_domain_name=context.user_domain_name,
                    auth_url=CONF.service_credentials.auth_url,
                    auth_token=context.auth_token,
                    insecure=CONF.nova_api_insecure)
    client.client.auth_token = context.auth_token
    client.client.endpoint_override = url
    return client


def create_admin_nova_client(context):
    """
    Creates client that uses trove admin credentials
    :return: a client for nova for the trove admin
    """
    client = create_nova_client(
        context, password=CONF.service_credentials.password
    )
    return client


def cinder_client(context, region_name=None):
    if CONF.cinder_url:
        url = '%(cinder_url)s%(tenant)s' % {
            'cinder_url': normalize_url(CONF.cinder_url),
            'tenant': context.project_id}
    else:
        region = region_name or CONF.service_credentials.region_name
        url = get_endpoint(
            context.service_catalog,
            service_type=CONF.cinder_service_type,
            endpoint_region=region,
            endpoint_type=CONF.cinder_endpoint_type
        )

    client = CinderClient.Client(context.user, context.auth_token,
                                 project_id=context.project_id,
                                 auth_url=CONF.service_credentials.auth_url,
                                 insecure=CONF.cinder_api_insecure)
    client.client.auth_token = context.auth_token
    client.client.management_url = url
    return client


def swift_client(context, region_name=None):
    if CONF.swift_url:
        # swift_url has a different format so doesn't need to be normalized
        url = '%(swift_url)s%(tenant)s' % {'swift_url': CONF.swift_url,
                                           'tenant': context.project_id}
    else:
        region = region_name or CONF.service_credentials.region_name
        url = get_endpoint(context.service_catalog,
                           service_type=CONF.swift_service_type,
                           endpoint_region=region,
                           endpoint_type=CONF.swift_endpoint_type)

    client = Connection(preauthurl=url,
                        preauthtoken=context.auth_token,
                        tenant_name=context.project_id,
                        snet=CONF.backup_use_snet,
                        insecure=CONF.swift_api_insecure)
    return client


def neutron_client(context, region_name=None):
    if CONF.neutron_url:
        # neutron endpoint url / publicURL does not include tenant segment
        url = CONF.neutron_url
    else:
        region = region_name or CONF.service_credentials.region_name
        url = get_endpoint(context.service_catalog,
                           service_type=CONF.neutron_service_type,
                           endpoint_region=region,
                           endpoint_type=CONF.neutron_endpoint_type)

    client = NeutronClient.Client(token=context.auth_token,
                                  endpoint_url=url,
                                  insecure=CONF.neutron_api_insecure)
    return client


def glance_client(context, region_name=None):

    # We should allow glance to get the endpoint from the service
    # catalog, but to do so we would need to be able to specify
    # the endpoint_filter on the API calls, but glance
    # doesn't currently allow that.  As a result, we must
    # specify the endpoint explicitly.
    if CONF.glance_url:
        endpoint_url = '%(url)s%(tenant)s' % {
            'url': normalize_url(CONF.glance_url),
            'tenant': context.project_id}
    else:
        region = region_name or CONF.service_credentials.region_name
        endpoint_url = get_endpoint(
            context.service_catalog, service_type=CONF.glance_service_type,
            endpoint_region=region,
            endpoint_type=CONF.glance_endpoint_type
        )

    auth = v3.Token(CONF.service_credentials.auth_url, context.auth_token)
    session = ka_session.Session(auth=auth)

    return glanceclient.Client(
        CONF.glance_client_version, endpoint=endpoint_url,
        session=session
    )


def create_dns_client(*arg, **kwargs):
    return import_class(CONF.remote_dns_client)(*arg, **kwargs)


def create_guest_client(*arg, **kwargs):
    return import_class(CONF.remote_guest_client)(*arg, **kwargs)


def create_nova_client(*arg, **kwargs):
    return import_class(CONF.remote_nova_client)(*arg, **kwargs)


def create_swift_client(*arg, **kwargs):
    return import_class(CONF.remote_swift_client)(*arg, **kwargs)


def create_cinder_client(*arg, **kwargs):
    return import_class(CONF.remote_cinder_client)(*arg, **kwargs)


def create_neutron_client(*arg, **kwargs):
    return import_class(CONF.remote_neutron_client)(*arg, **kwargs)


def create_glance_client(*arg, **kwargs):
    return import_class(CONF.remote_glance_client)(*arg, **kwargs)
