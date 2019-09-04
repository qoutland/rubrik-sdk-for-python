# Copyright 2018 Rubrik, Inc.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to
#  deal in the Software without restriction, including without limitation the
#  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
#  sell copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#  DEALINGS IN THE SOFTWARE.

"""
This module contains the Rubrik SDK ManagedVolume class.
"""
from .api import Api
from .exceptions import InvalidParameterException, CDMVersionException, InvalidTypeException

_Api = Api

class Managed_Volume(_Api):
    """This class contains methods for the management of managed volume functionality on the Rubrik Cluster"""

    def begin_managed_volume_snapshot(self, name, timeout=30):
        """Open a managed volume for writes. All writes to the managed volume until the snapshot is ended will be part of its snapshot.

        Arguments:
            name {str} -- The name of the Managed Volume to begin the snapshot on.

        Keyword Arguments:
            timeout {int} -- The number of seconds to wait to establish a connection the Rubrik cluster. (default: {30})

        Returns:
            str -- No change required. The Managed Volume '`name`' is already assigned in a writeable state.
            dict -- The full API response for `POST /managed_volume/{id}/begin_snapshot`.
        """

        self.log(
            "begin_managed_volume_snapshot: Searching the Rubrik cluster for the Managed Volume '{}'.".format(name))
        managed_volume_id = self.object_id(name, 'managed_volume', timeout=timeout)

        self.log("begin_managed_volume_snapshot: Determining the state of the Managed Volume '{}'.".format(name))
        managed_volume_summary = self.get('internal', '/managed_volume/{}'.format(managed_volume_id), timeout=timeout)

        if not managed_volume_summary['isWritable']:
            self.log(
                "begin_managed_volume_snapshot: Setting the Managed Volume '{}' to a writeable state.".format(name))
            return self.post('internal', '/managed_volume/{}/begin_snapshot'.format(managed_volume_id),
                             config={}, timeout=timeout)
        else:
            return "No change required. The Managed Volume '{}' is already assigned in a writeable state.".format(name)

    def end_managed_volume_snapshot(self, name, sla_name='current', timeout=30):
        """Close a managed volume for writes. A snapshot will be created containing all writes since the last begin snapshot call.

        Arguments:
            name {str} -- The name of the Managed Volume to end snapshots on.

        Keyword Arguments:
            sla_name {str} -- The SLA Domain name you want to assign the snapshot to. By default, the currently assigned SLA Domain will be used. (default: {'current'})
            timeout {int} -- The number of seconds to wait to establish a connection the Rubrik cluster. (default: {30})

        Returns:
            str -- No change required. The Managed Volume `name` is already assigned in a read only state.
            dict -- The full API response for `POST /managed_volume/{id}/end_snapshot`.
        """

        self.log("end_managed_volume_snapshot: Searching the Rubrik cluster for the Managed Volume '{}'.".format(name))
        managed_volume_id = self.object_id(name, 'managed_volume', timeout=timeout)

        self.log("end_managed_volume_snapshot: Determining the state of the Managed Volume '{}'.".format(name))
        managed_volume_summary = self.get("internal", "/managed_volume/{}".format(managed_volume_id), timeout=timeout)

        if not managed_volume_summary['isWritable']:
            return "No change required. The Managed Volume 'name' is already assigned in a read only state."

        if sla_name == 'current':
            self.log(
                "end_managed_volume_snapshot: Searching the Rubrik cluster for the SLA Domain assigned to the Managed Volume '{}'.".format(
                    name))
            if managed_volume_summary['slaAssignment'] == 'Unassigned' or managed_volume_summary[
                'effectiveSlaDomainId'] == 'UNPROTECTED':
                raise InvalidParameterException(
                    "The Managed Volume '{}' does not have a SLA assigned currently assigned. You must populate the sla_name argument.".format(
                        name))
            config = {}
        else:
            self.log(
                "end_managed_volume_snapshot: Searching the Rubrik cluster for the SLA Domain '{}'.".format(sla_name))
            sla_id = self.object_id(sla_name, 'sla', timeout=timeout)

            config = {}
            config['retentionConfig'] = {}
            config['retentionConfig']['slaId'] = sla_id

        return self.post("internal", "/managed_volume/{}/end_snapshot".format(managed_volume_id), config, timeout)

    def managed_volume_channels(self, name, timeout=15):
        """Gets the share details for the managed volume configured channels

                Arguments:
                    name {str} -- The name of the Managed Volume.

                Keyword Arguments:
                    timeout {int} -- The number of seconds to wait to establish a connection the Rubrik cluster. (default: {30})

                Returns:
                    list -- The list of managed volume channels  dict [{'ipAddress': 'ip', 'mountPoint': 'path'}].
                """
        managed_volume_details = self.get("internal", "/managed_volume?name={}".format(name), timeout=timeout)
        try:
            managed_volume_details = managed_volume_details["data"][0]["mainExport"]["channels"]
            return managed_volume_details
        except IndexError:
            raise IndexError("Managed volume not found.")

    def managed_volume_get_snapshot(self, managed_volume_name, date='latest', time='latest', timeout=15):
        """Gets the id of a snapshot based on the time of the snapshot. This can be the latest snapshot or it will be the closest to the provided
            time without going past that time.

               Arguments:
                   managed_volume_name {str} -- The name of the Managed Volume to export the snapshots from.

               Keyword Arguments:
                   date {str} -- The date of the desired snapshot. By default the latest snapshot is exported. (default: {'latest'})
                   time {str} -- The time of the desired snapshot. By default the latest snapshot is exported. (default: {'latest'})
                   timeout {int} -- The number of seconds to wait to establish a connection the Rubrik cluster. (default: {30})

               Returns:
                   str -- "No snapshot found"
                   str -- The snapshot id..
               """
        from datetime import datetime

        if date != 'latest' and time == 'latest' or date == 'latest' and time != 'latest':
            raise InvalidParameterException(
                "The date and time arguments most both be 'latest' or a specific date and time.")

        self.log("managed_volume_export_snapshot: Searching the Rubrik cluster for the Managed Volume '{}'.".format(managed_volume_name))
        mv_id = self.object_id(managed_volume_name, 'managed_volume', timeout=timeout)

        self.log("managed_volume_export_snapshot: Getting a list of all Snapshots for the Managed Volume '{}'.".format(managed_volume_name))
        mv_snapshot_summary = self.get('internal', '/managed_volume/{}/snapshot'.format(mv_id), timeout=timeout)

        if date == 'latest' and time == 'latest':
            self.log("managed_volume_export_snapshot: Retrieving latest snapshot for the Managed Volume '{}'.".format(managed_volume_name))
            number_of_snapshots = len(mv_snapshot_summary['data'])
            snapshot_id = mv_snapshot_summary['data'][number_of_snapshots - 1]['id']
        else:
            self.log("managed_volume_export_snapshot: Retrieving closest snapshot for the Managed Volume '{}'.".format(managed_volume_name))
            self.log("managed_volume_export_snapshot: Converting the provided date/time into UTC.")
            restore_date_time = datetime.strptime(self._date_time_conversion(date, time), '%Y-%m-%dT%H:%M')
            self.log("managed_volume_export_snapshot: Searching for the closest snapshot.")
            snapshot_list = {}
            for snapshot in mv_snapshot_summary['data']:
                snapshot_date_time = datetime.strptime(snapshot['date'], '%Y-%m-%dT%H:%M:%S.%fZ')
                if snapshot_date_time >= restore_date_time:
                    snapshot_list[snapshot['id']] = snapshot_date_time
            closest_snapshot = min(snapshot_list.values(), key=lambda x: abs(x - restore_date_time))
            self.log("managed_volume_export_snapshot: Retrieving ID for closest snapshot.")
            for closest_snapshot_id, snapshot_date in snapshot_list.items():
                if snapshot_date == closest_snapshot:
                    snapshot_id = closest_snapshot_id

        if not snapshot_id:
            snapshot_id = "No snapshot found"
            return snapshot_id

        return snapshot_id

    def managed_volume_export_snapshot(self, snapshot_id, host_patterns=['*'], share_type='NFS', allow_cloud=True, timeout=15):
        """Exports the requested managed volume snapshot.

               Arguments:
                   snapshot_id {str} -- The id of the snapshot to export.

               Keyword Arguments:
                   host_patterns {str} -- The hosts or ips able to mount the share. By default any host is allowed. (default: {'*'})
                   share_type {str} -- The protocol for the share. By default the share is exported over NFS. (default: {'NFS'})
                   timeout {int} -- The number of seconds to wait to establish a connection the Rubrik cluster. (default: {30})

               Returns:
                   str -- "Could not find snapshot with ID"
                   str --  "This snapshot is in the cloud and that has been disallowed in this request."
                   dict -- The full API response for `POST /managed_volume/snapshot/{id}/export`.
               """
        self.log("managed_volume_export_snapshot: Searching the Rubrik cluster for the snapshot id '{}'.".format(snapshot_id))
        snapshot_info = self.get('internal', '/managed_volume/snapshot/{}'.format(snapshot_id), timeout=timeout)
        if 'message' in snapshot_info:
            return snapshot_info['message']
        elif snapshot_info['cloudState'] == 1 and not allow_cloud:
            return "This snapshot is in the cloud and that has been disallowed in this request."

        config = {}
        config['hostPatterns'] = host_patterns
        config['shareType'] = share_type

        return self.post("internal", "/managed_volume/snapshot/{}/export".format(snapshot_id), config, timeout)

    def managed_volume_snapshot_export_info(self, snapshot_id, timeout=15):
        print(snapshot_id)
        self.log("managed_volume_snapshot_info: Checking the Rubrik cluster for the snapshot id '{}'.".format(snapshot_id))
        all_exports_info = self.get('internal', '/managed_volume/snapshot/export', timeout=timeout)
        print(all_exports_info)
        if snapshot_id in all_exports_info['data']:
            print(all_exports_info['data']['channels'])





