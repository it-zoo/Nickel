package models

import java.time.ZonedDateTime

class Message(val userId: String, var data: String, var time: ZonedDateTime) {
}