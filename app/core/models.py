from django.db import models
from django.forms import ValidationError


class XSRConfiguration(models.Model):
    """Model for XSR Configuration """

    source_file = models.FileField(help_text='Upload the source '
                                             'file')

    def save(self, *args, **kwargs):
        if not self.pk and XSRConfiguration.objects.exists():
            raise ValidationError('There can be only one XISConfiguration '
                                  'instance')
        return super(XSRConfiguration, self).save(*args, **kwargs)
