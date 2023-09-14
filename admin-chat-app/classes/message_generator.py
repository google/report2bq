# Copyright 2022 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import os
from typing import Any, List, Mapping

from card_framework.v2.action_response import ActionResponse
from card_framework.v2.card import Card
from card_framework.v2.card_fixed_footer import CardFixedFooter
from card_framework.v2.card_header import CardHeader
from card_framework.v2.dialog_action import Dialog, DialogAction
from card_framework.v2.message import Message
from card_framework.v2.section import Section
from card_framework.v2.widgets.action import Action, ActionParameter
from card_framework.v2.widgets.button import Button
from card_framework.v2.widgets.button_list import ButtonList
from card_framework.v2.widgets.decorated_text import DecoratedText
from card_framework.v2.widgets.divider import Divider
from card_framework.v2.widgets.grid import Grid, GridItem, ImageComponent
from card_framework.v2.widgets.icon import Icon
from card_framework.v2.widgets.on_click import OnClick
from card_framework.v2.widgets.selection_input import SelectionInput
from card_framework.v2.widgets.selection_item import SelectionItem
from card_framework.v2.widgets.text_input import TextInput

from classes import DictObj


class MessageGenerator(object):
  _edit = False
  _request = None

  @property
  def edit(self) -> bool:
    return self._edit

  @edit.setter
  def edit(self, edit: bool) -> None:
    self._edit = edit

  @property
  def request(self) -> DictObj:
    return self._request

  @request.setter
  def request(self, request: Mapping[str, Any]) -> None:
    self._request = DictObj(request)

  @property
  def job_data(self) -> DictObj:
    return self._job_data

  @job_data.setter
  def job_data(self, job_data: Mapping[str, Any]) -> None:
    self._job_data = DictObj(job_data)

  @property
  def project(self) -> str:
    return os.environ.get('GCP_PROJECT')

  @property
  def job_manager_uri(self) -> str:
    return os.environ.get('JOB_MANAGER_URI')

  def __init__(self, request: Mapping[str, Any]) -> MessageGenerator:
    """__init__ _summary_

    Args:
        request (Mapping[str, Any]): _description_

    Returns:
        MessageGenerator: _description_
    """
    self.request = request

  def error(self, message: str = "{command} is not a valid command.",
            **kwargs) -> Mapping[str, Any]:
    """error _summary_

    Args:
        message (str, optional): _description_. Defaults to "{command} is not a valid command.".

    Returns:
        Mapping[str, Any]: _description_
    """
    widgets = [DecoratedText(top_label='ERROR.',
                             text=message.format(**kwargs),
                             start_icon=Icon(known_icon=Icon.KnownIcon.STAR))]
    header = CardHeader(title='Error')
    card = Card(header=header, sections=[Section(widgets=widgets)])

    return Message(cards=[card]).render()

  def job_list(self,
               jobs: List[Mapping[str, Any]] = None) -> Mapping[str, Any]:
    job_list = list()

    if jobs:
      for job in jobs:
        name = job['name'].split('/')[-1]
        job_list.append(
            DecoratedText(top_label=f'{name}',
                          text=f"{job.get('description', '-- No description --')}",
                          on_click=OnClick(
                              action=Action(
                                  function='fetch_job',
                                  parameters=[
                                    ActionParameter(key='job_id',
                                                    value=job['name'])]))))

    else:
      job_list.append(
          DecoratedText(text=f"No jobs found for {self.request.user.email}.",
                        start_icon=Icon(known_icon=Icon.KnownIcon.STAR)))

    header = CardHeader(title=f'Jobs for {self.request.user.email}')

    card = Card(
        header=header,
        sections=[
            Section(widgets=job_list)])

    return Message(cards=[card]).render()

  def fetch_job(self, job: Mapping[str, Any]) -> Mapping[str, Any]:
    job_id = job['name']

    header = CardHeader(title=f"{job['description']}",
                        subtitle=f"{job_id}")

    widgets = list()
    attributes = list()
    schedule = list()
    job_attributes = job['pubsub_target']['attributes']

    widgets.append(DecoratedText(
        top_label='Type',
        text=job_attributes['type'],
        start_icon=Icon(known_icon=Icon.KnownIcon.CONFIRMATION_NUMBER_ICON)))

    for attribute in job_attributes.keys():
      attributes.append(DecoratedText(
          top_label=f"{attribute}",
          text=f"{job_attributes[attribute]}",
          start_icon=Icon(known_icon=Icon.KnownIcon.DESCRIPTION)))

    schedule.append(DecoratedText(
        top_label="Schedule",
        text=f"{job['schedule']}",
        start_icon=Icon(known_icon=Icon.KnownIcon.CLOCK)))

    button_parameters = [ActionParameter(key='job_id', value=job_id)]

    match job['state']:
      case 1 | 'ENABLED':
        primary = Button(text='DISABLE',
                              on_click=OnClick(action=Action(
                                  function='disable_job',
                                  parameters=button_parameters)))
      case 2 | 3 | 'PAUSED' | 'DISABLED':
        primary = Button(text='ENABLE',
                              on_click=OnClick(action=Action(
                                  function='enable_job',
                                  parameters=button_parameters)))
      case 0 | 4 | 'STATE_UNSPECIFIED' | 'UPDATE_FAILED':
        primary = Button(text='DO NOT CLICK THIS',
                              on_click=OnClick(action=Action(function='')))

    secondary = Button(text='EDIT',
                       on_click=OnClick(action=Action(
                           function='edit_job',
                           interaction=Action.Interaction.OPEN_DIALOG,
                           parameters=button_parameters)))

    tertiary = Button(text='DELETE',
                      on_click=OnClick(action=Action(
                           function='delete_job',
                           parameters=button_parameters)))
    button_list = ButtonList(buttons=[primary, secondary, tertiary])

    card = Card(header=header, sections=[
        Section(widgets=widgets),
        Section(widgets=attributes),
        Section(widgets=schedule),
        Section(widgets=[button_list])])

    return Message(cards=[card]).render()

  # Creating jobs
  @property
  def report_options(self) -> Section:
    options = self.job_data.pubsub_target.attributes if self.edit else DictObj()

    section = Section(
        header='OPTIONS',
        widgets=[SelectionInput(
            label='Extra report Options',
            name='options',
            type=SelectionInput.SelectionType.SWITCH,
            items=[
                SelectionItem(text='Append instead of overwrite',
                              value='append',
                              selected=bool(options.append)),
                SelectionItem(text='Force update every run',
                              value='force',
                              selected=bool(options.force)),
                SelectionItem(text='Rebuild schema every run',
                              value='rebuild',
                              selected=bool(options.rebuild)),
            ])])

    return section

  @property
  def table_options(self) -> Section:
    source = self.job_data.pubsub_target.attributes if self.edit else self.request.common.formInputs

    section = Section(
        header='OPTIONAL TABLE SETTINGS',
        collapsible=True,
        widgets=[
            # --partition   Store the table in Big Query as a date-partitioned table. This means you MUST
            #               have in your schema at least one DATE or DATETIME column. The FIRST ONE in the
            #               schema will be the one used to partition the data. Using:
            #                 --partition=timestamp
            #               will cause the system to partition on 'ingestion' time; see the Big Query
            #               documentation for an explanation for this.
            SelectionInput(
                type=SelectionInput.SelectionType.DROPDOWN,
                label="Partition dataset?",
                name="partition",
                items=[
                    SelectionItem(text=("Timestamp - based on the ingestion "
                                        "time of the data"),
                                  value="timestamp",
                                  selected=False),
                    SelectionItem(text=("Inferred - based on the FIRST date "
                                        "or datetime column in the data"),
                                  value="infer",
                                  selected=False),
                ]
            ),
            Divider(),
            # --dest-project
            # Destination GCP project (if different than "--project")
            TextInput(label="Destination project",
                      type=TextInput.Type.SINGLE_LINE,
                      name="dest_project",
                      hint_text=f"Destination project, if other than '{self.project}'",
                      value=source.dest_project),

            # --dest-dataset
            #               Destination BQ dataset (if not 'report2bq')
            TextInput(label="Destination dataset",
                      type=TextInput.Type.SINGLE_LINE,
                      name="dest_dataset",
                      hint_text="Destination dataset, if other than 'report2bq'",
                      value=source.dest_dataset),
            # --dest-table OR --alias
            TextInput(label="Destination table alias",
                      type=TextInput.Type.SINGLE_LINE,
                      name="dest_table",
                      hint_text="Destination table name, if desired.",
                      value=source.dest_table),
        ]
    )

    return section

  @property
  def minute(self) -> TextInput:
    if self.edit:
      minute = self.job_data.schedule.split(' ')[0]
    else:
      minute = ''

    return TextInput(label="Minute (optional)",
                     type=TextInput.Type.SINGLE_LINE,
                     name="minute",
                     hint_text=("The minute in the hour the job will run. "
                                "If not defined, a random minute will be"
                                " assigned"),
                     value=minute)

  @property
  def hour(self) -> TextInput:
    if self.edit:
      hour = self.job_data.schedule.split(' ')[1]
      if hour == '*':
        hour = ''
    else:
      hour = ''

    return TextInput(label="hour (optional)",
                     type=TextInput.Type.SINGLE_LINE,
                     name="hour",
                     hint_text=(
                         "The hour the job will run. For a DV360/CM fetcher,"
                         "this is '*', or 'every hour' at 'timer' minute and"
                         "cannot be changed. For ADH this defaults to 2. "
                         "For SA360 this defaults to 3. "
                         "For all report runners this defaults to 1"),
                     value=hour)

  # TODO: to be added?
  # --time-zone   Timezone for the job. Default is the value in /etc/timezone, or UTC if that file is
  #               not present. If you type it manually, the value of this field must be a time zone
  # name from the TZ database (http://en.wikipedia.org/wiki/Tz_database)

  @property
  def post_processor(self) -> Section:
    source = self.job_data.pubsub_target.attributes if self.edit else self.request.common.formInputs

    notify_message = TextInput(
        label="Postprocessor function",
        type=TextInput.Type.SINGLE_LINE,
        name="notify_message",
        hint_text=("Message to send; this should be the "
                   "name of the custom function to be "
                   "executed. Attributes of dataset, "
                   "table name, report id and report "
                   "type will always be sent along with "
                   "this as part of the message"),
        value=source.notify_message)

    return Section(header='Post-processing',
                   widgets=[notify_message])

  @property
  def pre_selected(self) -> Section:
    source = self.job_data.pubsub_target.attributes if self.edit else self.request.common.formInputs

    return Section(
        header='PREVIOUS ENTRIES',
        collapsible=True,
        widgets=[SelectionInput(
            label='Previously entered data',
            name='prior',
            type=SelectionInput.SelectionType.SWITCH,
            items=[
                SelectionItem(
                  text=f'Project: {source.project if self.edit else source.project.stringInputs.value}',
                    value=source.project if self.edit else source.project.stringInputs.value,
                    selected=True),
                SelectionItem(
                  text=f'Owner email: {source.email if self.edit else source.email.stringInputs.value}',
                    value=source.email if self.edit else source.email.stringInputs.value,
                    selected=True),
                SelectionItem(
                  text=f'Product: {source.type if self.edit else "product"}',
                    value=source.type if self.edit else 'product',
                    selected=True),
            ])])

  @property
  def description(self) -> TextInput:
    source: DictObj = self.job_data if self.edit else self.request.common.formInputs
    return TextInput(label='Description',
                     type=TextInput.Type.SINGLE_LINE,
                     name="description",
                     hint_text=("A plain text description that will "
                                "appear in the scheduler list."),
                     value=source.description)

  @property
  def report_type(self) -> SelectionInput:
    return SelectionInput(
        type=SelectionInput.SelectionType.DROPDOWN,
        label="Fetcher or Runner?",
        name="runner",
        items=[
            SelectionItem(text="Fetcher", value="fetcher", selected=True),
            SelectionItem(text="Runner", value="runner", selected=False),
        ]
    )

  def create_new_job(self) -> Mapping[str, Any]:
    # dialog to enter fields
    email = self.request.user.email

    header = CardHeader(title="Create a new Report2BQ job")
    sections = list()
    widgets = list()
    project = TextInput(label="GCP Project",
                        type=TextInput.Type.SINGLE_LINE,
                        name="project",
                        value=f"{self.project}")
    owner = TextInput(label="Owner email",
                      type=TextInput.Type.SINGLE_LINE,
                      name="email",
                      value=f"{email}")
    product = SelectionInput(
        type=SelectionInput.SelectionType.DROPDOWN,
        label="GMP Product",
        name="product",
        items=[
            SelectionItem(text="DisplayVideo 360 (DV360)",
                          value="dv360", selected=True),
            SelectionItem(text="Campaign Manager (CM360)",
                          value="cm", selected=False),
            SelectionItem(text="Search Ads 360 (SA360)",
                          value="sa360", selected=False),
            SelectionItem(text="Analytics (GA360)",
                          value="ga360", selected=False),
            SelectionItem(text='Ads Data Hub (ADH)',
                          value='adh', selected=False),
        ]
    )
    _product = Grid(
        title='Product Selection',
        items=[
            GridItem(title='DisplayVideo 360', subtitle='DV360',
                     image=ImageComponent(image_uri='https://developers.google.com/ads/images/logo_display_video_360_192px.svg',
                                          alt_text='DV360 Logo')),
            GridItem(title='Campaign Manager 360', subtitle='CM360',
                     image=ImageComponent(image_uri='https://developers.google.com/ads/images/logo_campaign_manager_192px.svg',
                                          alt_text='CM360 Logo')),
            GridItem(title='Search Ads 360', subtitle='SA360',
                     image=ImageComponent(image_uri='https://www.gstatic.com/images/branding/product/1x/search_ads_360_24dp.png',
                                          alt_text='SA360 Logo')),
            GridItem(title='Ads Data Hub', subtitle='ADH',
                     image=ImageComponent(image_uri='https://www.gstatic.com/images/branding/product/1x/ads_data_hub_24dp.png',
                                          alt_text='ADH Logo')),
        ]
    )

    widgets = [project, owner, product]
    sections = [Section(widgets=widgets)]
    card = Card(
        header=header, sections=sections, name='create_job',
        fixed_footer=CardFixedFooter(
            primary_button=Button(text="Next",
                                  on_click=OnClick(
                                      action=Action(
                                          function='new_job_details')))))

    dialog_action = DialogAction(dialog=Dialog(body=card))
    response = ActionResponse(type=ActionResponse.ResponseType.DIALOG,
                              dialog_action=dialog_action)

    return response.render()

  def job_details_dv360(self) -> Mapping[str, Any]:
    header = CardHeader(title="Report2BQ DV360 job: details")

    report_id = TextInput(
        label="Report Id",
        type=TextInput.Type.SINGLE_LINE,
        name="report_id",
        hint_text="",
        value=(
            self.job_data.pubsub_target.attributes.dv360_id or
            self.job_data.pubsub_target.attributes.report_id)
        if self.edit else self.request.report_id)

    sections = [
        self.pre_selected,
        Section(header='REPORT DETAILS', widgets=[
                self.description, self.report_type, report_id,
                self.hour, self.minute]),
        self.post_processor,
        self.table_options,
        self.report_options,
    ]
    card = Card(
        name='create_job',
        header=header,
        sections=sections,
        fixed_footer=CardFixedFooter(
            primary_button=Button(
                text='UPDATE' if self.edit else 'CREATE',
                on_click=OnClick(
                    action=Action(
                        function='update_job' if self.edit else 'create_job')
                )))
    )

    dialog_action = DialogAction(dialog=Dialog(body=card))
    response = ActionResponse(type=ActionResponse.ResponseType.DIALOG,
                              dialog_action=dialog_action)

    return response.render()

  def job_details_cm360(self) -> Mapping[str, Any]:
    header = CardHeader(title="Report2BQ CM360 job: details")

    profile = TextInput(label='Profile Id',
                        type=TextInput.Type.SINGLE_LINE,
                        name="profile",
                        hint_text=(
                            "Campaign Manager profile id under which to run the report. "
                            "The creator's email must have access to this profile."),
                        value=(
                            self.job_data.pubsub_target.attributes.profile
                            if self.edit else self.request.profile))

    report_id = TextInput(label='Report Id',
                          type=TextInput.Type.SINGLE_LINE,
                          name='report_id',
                          hint_text='The id of the CM360 report you want to run',
                          value=(
                              self.job_data.pubsub_target.attributes.cm_id or
                              self.job_data.pubsub_target.attributes.report_id)
                          if self.edit else self.request.report_id)

    sections = [
        self.pre_selected,
        Section(header='REPORT DETAILS', widgets=[
                self.description, self.report_type, report_id, profile,
                self.hour, self.minute]),
        self.post_processor,
        self.report_options,
        self.table_options,
    ]
    card = Card(
        name='create_job',
        header=header,
        sections=sections,
        fixed_footer=CardFixedFooter(
            primary_button=Button(
                text='UPDATE' if self.edit else 'CREATE',
                on_click=OnClick(
                    action=Action(
                        function='update_job' if self.edit else 'create_job')
                )))
    )

    dialog_action = DialogAction(dialog=Dialog(body=card))
    response = ActionResponse(type=ActionResponse.ResponseType.DIALOG,
                              dialog_action=dialog_action)

    return response.render()

  def job_details_sa360(self) -> Mapping[str, Any]:
    header = CardHeader(title="Report2BQ SA360 job: details")

    sa360_url = TextInput(label="SA360 Web Downloadable Report URL",
                          type=TextInput.Type.SINGLE_LINE,
                          name="sa360_url",
                          hint_text=(
                              "The URL of the web download report in SA360. This"
                              " will be in the format https://searchads.google"
                              ".com/ds/reports/download?ay=xxxxxxxxx&av=0&"
                              "rid=000000&of=webquery"),
                          value=(
                              self.job_data.pubsub_target.attributes.sa360_url
                              if self.edit else ''))

    _or = DecoratedText(text=' OR ')

    report_id = TextInput(label="SA360 Dynamic Report UUID",
                          type=TextInput.Type.SINGLE_LINE,
                          name="report_id",
                          hint_text="The UUID of the created SA360 report",
                          value=(
                              self.job_data.pubsub_target.attributes.sa360_url
                              if self.edit else ''))

    sections = [
        self.pre_selected,
        Section(header='REPORT DETAILS',
                widgets=[self.description, report_id, _or, sa360_url,
                         self.hour, self.minute]),
        self.post_processor,
        self.report_options,
        self.table_options,
    ]
    card = Card(
        name='create_job',
        header=header,
        sections=sections,
        fixed_footer=CardFixedFooter(
            primary_button=Button(
                text='UPDATE' if self.edit else 'CREATE',
                on_click=OnClick(
                    action=Action(
                        function='update_job' if self.edit else 'create_job')
                ))))

    dialog_action = DialogAction(dialog=Dialog(body=card))
    response = ActionResponse(type=ActionResponse.ResponseType.DIALOG,
                              dialog_action=dialog_action)

    return response.render()
