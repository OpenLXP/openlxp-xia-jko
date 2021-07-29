# Generated by Django 3.1.13 on 2021-07-29 21:26

from django.db import migrations, models
import django.utils.timezone
import model_utils.fields
import uuid


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='MetadataFieldOverwrite',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', model_utils.fields.AutoCreatedField(default=django.utils.timezone.now, editable=False, verbose_name='created')),
                ('modified', model_utils.fields.AutoLastModifiedField(default=django.utils.timezone.now, editable=False, verbose_name='modified')),
                ('field_name', models.CharField(max_length=200)),
                ('field_type', models.CharField(choices=[('datetime', 'DATETIME'), ('int', 'INTEGER'), ('char', 'CHARACTER'), ('bool', 'BOOLEAN'), ('txt', 'TEXT')], max_length=200)),
                ('field_value', models.CharField(max_length=200)),
                ('overwrite', models.BooleanField()),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='MetadataLedger',
            fields=[
                ('metadata_record_inactivation_date', models.DateTimeField(blank=True, null=True)),
                ('metadata_record_uuid', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('record_lifecycle_status', models.CharField(blank=True, choices=[('Active', 'A'), ('Inactive', 'I')], max_length=10)),
                ('source_metadata', models.JSONField(blank=True)),
                ('source_metadata_extraction_date', models.DateTimeField(auto_now_add=True)),
                ('source_metadata_hash', models.CharField(max_length=200)),
                ('source_metadata_key', models.TextField()),
                ('source_metadata_key_hash', models.CharField(max_length=200)),
                ('source_metadata_transformation_date', models.DateTimeField(blank=True, null=True)),
                ('source_metadata_validation_date', models.DateTimeField(blank=True, null=True)),
                ('source_metadata_validation_status', models.CharField(blank=True, choices=[('Y', 'Yes'), ('N', 'No')], max_length=10)),
                ('target_metadata', models.JSONField(default=dict)),
                ('target_metadata_hash', models.CharField(max_length=200)),
                ('target_metadata_key', models.TextField()),
                ('target_metadata_key_hash', models.CharField(max_length=200)),
                ('target_metadata_transmission_date', models.DateTimeField(blank=True, null=True)),
                ('target_metadata_transmission_status', models.CharField(blank=True, choices=[('Successful', 'S'), ('Failed', 'F'), ('Pending', 'P'), ('Ready', 'R')], default='Ready', max_length=10)),
                ('target_metadata_transmission_status_code', models.IntegerField(blank=True, null=True)),
                ('target_metadata_validation_date', models.DateTimeField(blank=True, null=True)),
                ('target_metadata_validation_status', models.CharField(blank=True, choices=[('Y', 'Yes'), ('N', 'No')], max_length=10)),
            ],
        ),
        migrations.CreateModel(
            name='ReceiverEmailConfiguration',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('email_address', models.EmailField(help_text='Enter email personas addresses to send log data', max_length=254, unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='SenderEmailConfiguration',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('sender_email_address', models.EmailField(default='openlxphost@gmail.com', help_text='Enter sender email address to send log data from', max_length=254)),
            ],
        ),
        migrations.CreateModel(
            name='SupplementalLedger',
            fields=[
                ('metadata_record_inactivation_date', models.DateTimeField(blank=True, null=True)),
                ('metadata_record_uuid', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('record_lifecycle_status', models.CharField(blank=True, choices=[('Active', 'A'), ('Inactive', 'I')], max_length=10)),
                ('supplemental_metadata', models.JSONField(blank=True)),
                ('supplemental_metadata_extraction_date', models.DateTimeField(auto_now_add=True)),
                ('supplemental_metadata_hash', models.CharField(max_length=200)),
                ('supplemental_metadata_key', models.TextField()),
                ('supplemental_metadata_key_hash', models.CharField(max_length=200)),
                ('supplemental_metadata_transformation_date', models.DateTimeField(blank=True, null=True)),
                ('supplemental_metadata_transmission_date', models.DateTimeField(blank=True, null=True)),
                ('supplemental_metadata_transmission_status', models.CharField(blank=True, choices=[('Successful', 'S'), ('Failed', 'F'), ('Pending', 'P'), ('Ready', 'R')], default='Ready', max_length=10)),
                ('supplemental_metadata_transmission_status_code', models.IntegerField(blank=True, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='XIAConfiguration',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('publisher', models.CharField(default='JKO', help_text='Enter the publisher name', max_length=200)),
                ('source_metadata_schema', models.CharField(default='JKO_source_validate_schema.json', help_text='Enter the JKO schema file', max_length=200)),
                ('source_target_mapping', models.CharField(default='JKO_p2881_target_metadata_schema.json', help_text='Enter the schema file to map target.', max_length=200)),
                ('target_metadata_schema', models.CharField(default='p2881_target_validation_schema.json', help_text='Enter the target schema file to validate from.', max_length=200)),
            ],
        ),
        migrations.CreateModel(
            name='XISConfiguration',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('xis_metadata_api_endpoint', models.CharField(help_text='Enter the XIS Metadata Ledger API endpoint', max_length=200)),
                ('xis_supplemental_api_endpoint', models.CharField(help_text='Enter the XIS Supplemental Ledger API endpoint', max_length=200)),
            ],
        ),
        migrations.CreateModel(
            name='XSRConfiguration',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_file', models.FileField(help_text='Upload the source file', upload_to='')),
            ],
        ),
    ]
