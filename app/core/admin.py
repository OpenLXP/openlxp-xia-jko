from django.contrib import admin

from .models import (MetadataFieldOverwrite, ReceiverEmailConfiguration,
                     SenderEmailConfiguration, XIAConfiguration,
                     XISConfiguration, XSRConfiguration)

# Register your models here.


@admin.register(XIAConfiguration)
class XIAConfigurationAdmin(admin.ModelAdmin):
    list_display = (
        'publisher',
        'source_metadata_schema',
        'source_target_mapping',
        'target_metadata_schema',)
    fields = ['publisher',
              'source_metadata_schema',
              ('source_target_mapping',
               'target_metadata_schema')]


@admin.register(XISConfiguration)
class XISConfigurationAdmin(admin.ModelAdmin):
    list_display = ('source_file',)
    fields = ['source_file']


@admin.register(XSRConfiguration)
class XSRConfigurationAdmin(admin.ModelAdmin):
    list_display = (
        'xsr_api_endpoint',
        'token_url',
        'edx_client_id',
        'edx_client_secret',)
    fields = ['xsr_api_endpoint',
              'token_url',
              'edx_client_id',
              'edx_client_secret']


@admin.register(ReceiverEmailConfiguration)
class ReceiverEmailConfigurationAdmin(admin.ModelAdmin):
    list_display = ('email_address',)


@admin.register(SenderEmailConfiguration)
class SenderEmailConfigurationAdmin(admin.ModelAdmin):
    list_display = ('sender_email_address',)


@admin.register(MetadataFieldOverwrite)
class MetadataFieldOverwriteAdmin(admin.ModelAdmin):
    list_display = ('field_name',
                    'field_type',
                    'field_value',
                    'overwrite',)
    fields = ['field_name',
              'field_type',
              'field_value',
              'overwrite']
