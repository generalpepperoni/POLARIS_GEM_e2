#!/usr/bin/env python3
import os
import argparse
from datetime import datetime
from typing import List, Dict

import rospy
from std_msgs.msg import Float32, Header


class CTDataCollector():
    def __init__(self, dur: int = 60, persist: bool = False):
        # Class variables init
        self.duration = rospy.Duration(dur)
        self.start_time = None
        self.latest_header = None
        self.complete_flag = False
        self.persist_flag = persist
        self.cte_list = []

        # Init blank Average Crosstrack error publisher
        self.avg_pub = rospy.Publisher(
            '/gem/metrics/ct_error_avg_last',
            Float32,
            queue_size=10,
            latch=True
        )
        # Also store subscribers as an instance Vars
        self.header_sub = rospy.Subscriber(
            "/gem/metrics/ct_error_header",
            Header,
            self.header_callback,
            queue_size=10,
        )
        self.error_sub = rospy.Subscriber(
            "/gem/metrics/ct_error",
            Float32,
            self.ct_callback,
            queue_size=10,
        )
        rospy.loginfo("CTE Subscribers initialized")

    def header_callback(self, msg):
        self.latest_header = msg
        if self.start_time is None:
            self.start_time = msg.stamp
            rospy.loginfo(f"CT error collection started at sim time: {self.start_time.to_sec():.4f}")

    def ct_callback(self, msg):
        if self.complete_flag:
            return

        if self.latest_header is None:
            rospy.logwarn_once("Waiting for first CTE header message...")
            return

        os_time = datetime.now()
        msg_time = self.latest_header.stamp
        test_time = msg_time - self.start_time
        self.cte_list.append({
            "ct_seq": self.latest_header.seq,
            "ct_error": msg.data,
            "msg_time": msg_time.to_sec(),
            "ros_time": rospy.Time.now().to_sec(),
            "os_ts": os_time.timestamp(),
            "os_ts_iso": os_time.isoformat(),
        })

        rospy.loginfo_throttle(3.0, f"Collected {len(self.cte_list)} samples | Last CTE: {msg.data:.3f}")

        if test_time >= self.duration:
            self.complete_flag = True
            rospy.loginfo(f"CTE collected! Lasted {test_time.to_sec()}s Sim time | SAMPLES: {len(self.cte_list)}")
            self.calc_publish_average(persist=self.persist_flag)
            rospy.signal_shutdown("OK")

    def calc_publish_average(self, persist: bool = False):
        """Publish the average CTE before ros shutdown"""
        try:
            avg_error = sum(row['ct_error'] for row in self.cte_list) / len(self.cte_list)
            avg_error_float = Float32(avg_error)
            self.avg_pub.publish(avg_error_float)
            if persist:
                log_path = '/var/log/ros/gem_crosstrack_error_last'
                try:
                    with open(log_path, 'w') as f:
                        f.write(str(avg_error_float))
                    rospy.loginfo(f"Crosstrack average error {str(avg_error_float)} saved to: {log_path}")
                except IOError as e:
                    rospy.logwarn(f"Unable to persist average CTE to {log_path}  : {e}")

            rospy.loginfo(f"Published average CTE: {avg_error}")
        except rospy.ROSInterruptException:
            pass

    def get_cte_data(self):
        return self.cte_list


def write_dicts_to_csv(
        data: List[Dict],
        filename: str = "/tmp/ros_gem_ct_errors.csv"
):
    import csv

    if not data:
        rospy.logerr("No data to write!")
        return
    with open(filename, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    rospy.loginfo(f"Data saved to {filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Crosstrack error validation script')
    parser.add_argument('-p', '--persist', action='store_true', help='Save .csv results to filesystem')
    parser.add_argument('-f', '--file', default='/tmp/ros_gem_ct_errors.csv', type=str, help='Output file')
    parser.add_argument(
        '-d', '--duration', default=30, type=int,
        help='Measurement duration inside simulation, in seconds (default: 30)',
    )

    cli_args = parser.parse_args()

    PERSIST_FLAG = cli_args.persist if not None else True
    DURATION_SEC = cli_args.duration if not None else 60
    OUTPUT_FILE = cli_args.file if not None else '/tmp/ros_gem_ct_errors.csv'

    # Starting node
    rospy.init_node('gem_ct_error_collector')
    rospy.set_param('use_sim_time', True)

    # Pre-run sanity checks
    if DURATION_SEC <= 0 or DURATION_SEC > 9000:
        rospy.logerr('Measurement duration cannot be negative or too high (over 9000)')
        exit(1)
    if os.path.exists(OUTPUT_FILE):
        rospy.logwarn(f"CSV {OUTPUT_FILE} exists on filesystem! It will be overwritten!")

    # Main collection loop
    collector = CTDataCollector(dur=DURATION_SEC, persist=PERSIST_FLAG)
    rospy.spin()

    # Processing CTE data after rospy.shutdown()
    cte_data = collector.get_cte_data()
    # rospy.log(pprint(cte_data[:2], indent=2, width=40))

    if PERSIST_FLAG:
        write_dicts_to_csv(cte_data)
