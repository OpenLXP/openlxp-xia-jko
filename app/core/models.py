import uuid

from django.db import models
from django.forms import ValidationError
from django.urls import reverse

# def user_directory_path(instance, filename):
#     # file will be uploaded to MEDIA_ROOT / user_<id>/<filename>
#     return 'user_{0}/{1}'.format(instance.user.id, filename)


class XIAConfiguration(models.Model):
    """Model for XIA Configuration """
    publisher = models.CharField(default='JKO', max_length=200,
                                 help_text='Enter the publisher name')
    source_metadata_schema = models.CharField(
        default='JKO_source_validate_schema.json', max_length=200,
        help_text='Enter the JKO '
                  'schema file')
    source_target_mapping = models.CharField(
        default='JKO_p2881_target_metadata_schema.json', max_length=200,
        help_text='Enter the schema '
                  'file to map '
                  'target.')
    target_metadata_schema = models.CharField(
        default='p2881_target_validation_schema.json', max_length=200,
        help_text='Enter the target '
                  'schema file to '
                  'validate from.')
    source_file = models.FileField(blank=True, help_text='Upload the source '
                                                         'file')

    def get_absolute_url(self):
        """ URL for displaying individual model records."""
        return reverse('Configuration-detail', args=[str(self.id)])

    def __str__(self):
        """String for representing the Model object."""
        return f'{self.id}'

    def save(self, *args, **kwargs):
        if not self.pk and XIAConfiguration.objects.exists():
            raise ValidationError('There is can be only one XIAConfiguration '
                                  'instance')
        return super(XIAConfiguration, self).save(*args, **kwargs)


class MetadataLedger(models.Model):
    """Model for MetadataLedger """

    METADATA_VALIDATION_CHOICES = [('Y', 'Yes'), ('N', 'No')]
    RECORD_ACTIVATION_STATUS_CHOICES = [('Active', 'A'), ('Inactive', 'I')]
    RECORD_TRANSMISSION_STATUS_CHOICES = [('Successful', 'S'), ('Failed', 'F'),
                                          ('Pending', 'P'), ('Ready', 'R')]

    metadata_record_inactivation_date = models.DateTimeField(blank=True,
                                                             null=True)
    metadata_record_uuid = models.UUIDField(primary_key=True,
                                            default=uuid.uuid4, editable=False)
    record_lifecycle_status = models.CharField(max_length=10, blank=True,
                                               choices=
                                               RECORD_ACTIVATION_STATUS_CHOICES)
    source_metadata = models.JSONField(blank=True)
    source_metadata_extraction_date = models.DateTimeField(auto_now_add=True)
    source_metadata_hash = models.CharField(max_length=200)
    source_metadata_key = models.TextField()
    source_metadata_key_hash = models.CharField(max_length=200)
    source_metadata_transformation_date = models.DateTimeField(blank=True,
                                                               null=True)
    source_metadata_validation_date = models.DateTimeField(blank=True,
                                                           null=True)
    source_metadata_validation_status = models.CharField(max_length=10,
                                                         blank=True,
                                                         choices=
                                                         METADATA_VALIDATION_CHOICES)
    target_metadata = models.JSONField(default=dict)
    target_metadata_hash = models.CharField(max_length=200)
    target_metadata_key = models.TextField()
    target_metadata_key_hash = models.CharField(max_length=200)

    target_metadata_transmission_date = models.DateTimeField(blank=True,
                                                             null=True)
    target_metadata_transmission_status = models.CharField(max_length=10,
                                                           blank=True,
                                                           default='Ready',
                                                           choices=
                                                           RECORD_TRANSMISSION_STATUS_CHOICES)
    target_metadata_transmission_status_code = models.CharField(max_length=200,
                                                                blank=True)
    target_metadata_validation_date = models.DateTimeField(blank=True,
                                                           null=True)
    target_metadata_validation_status = models.CharField(max_length=10,
                                                         blank=True,
                                                         choices=
                                                         METADATA_VALIDATION_CHOICES)
