from django.contrib import admin

from .models import XIAConfiguration

# Register your models here.


@admin.register(XIAConfiguration)
class XIAConfigurationAdmin(admin.ModelAdmin):
    list_display = (
        'publisher',
        'source_metadata_schema',
        'source_target_mapping',
        'target_metadata_schema',
        'source_file',)
    fields = ['publisher',
              'source_metadata_schema',
              ('source_target_mapping',
               'target_metadata_schema',
               'source_file')]
