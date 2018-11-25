package berlin.intero.sentientlighthubmessageprocessor.scheduled

import berlin.intero.sentientlighthub.common.SentientProperties
import berlin.intero.sentientlighthub.common.model.MQTTEvent
import berlin.intero.sentientlighthub.common.model.actor.EStrip
import berlin.intero.sentientlighthub.common.model.actor.ETreeSide
import berlin.intero.sentientlighthub.common.model.payload.SingleLEDPayload
import berlin.intero.sentientlighthub.common.model.payload.TreePixelPayload
import berlin.intero.sentientlighthub.common.services.ConfigurationService
import berlin.intero.sentientlighthub.common.tasks.MQTTPublishAsyncTask
import berlin.intero.sentientlighthub.common.tasks.MQTTSubscribeAsyncTask
import com.google.gson.Gson
import com.google.gson.JsonSyntaxException
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.core.task.SyncTaskExecutor
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.*
import java.util.logging.Logger

/**
 * This scheduled task
 * <li> calls {@link MQTTSubscribeAsyncTask} to subscribe sensor values from MQTT broker
 * <li> calls {@link SentientMappingEvaluationAsyncTask} for each mapping from configuration
 */
@Component
class MessageProcessingScheduledTaskTree {
    val values: MutableMap<String, String> = HashMap()
    val valuesHistoric: MutableMap<String, String> = HashMap()

    companion object {
        private val log: Logger = Logger.getLogger(MessageProcessingScheduledTaskTree::class.simpleName)
    }

    init {
        val topic = "${SentientProperties.MQTT.Topic.Tree.SET_PIXEL}/#"

        val callback = object : MqttCallback {
            override fun messageArrived(topic: String, message: MqttMessage) {
                log.fine("MQTT value receiced")

                // Parse payload
                val payload = String(message.payload)
                val value = Gson().fromJson(payload, TreePixelPayload::class.java)

                // Determine identifier
                val identifier = " ${value.side}/${value.strip}/${value.pixel}"

                values[identifier] = payload
            }

            override fun connectionLost(cause: Throwable?) {
                log.fine("MQTT connection lost")
            }

            override fun deliveryComplete(token: IMqttDeliveryToken?) {
                log.fine("MQTT delivery complete")
            }
        }

        // Call MQTTSubscribeAsyncTask
        SimpleAsyncTaskExecutor().execute(MQTTSubscribeAsyncTask(topic, callback))
    }

    @Scheduled(fixedDelay = SentientProperties.Frequency.SENTIENT_MESSAGE_PROCESS_DELAY)
    @SuppressWarnings("unused")
    fun write() {
        log.fine("${SentientProperties.Color.TASK}-- MESSAGE PROCESSING TASK${SentientProperties.Color.RESET}")

        values.forEach { identifier, payload ->

            // Parse payload
            val value = Gson().fromJson(payload, TreePixelPayload::class.java)

            try {
                val stripId = ConfigurationService.getTreeStripId(ETreeSide.valueOf(value.side.toUpperCase()), EStrip.valueOf(value.strip.toUpperCase()))
                val ledId = value.pixel
                val warmWhite = value.colors.warmWhite
                val coldWhite = value.colors.coldWhite
                val amber = value.colors.amber

                val actor = ConfigurationService.getActor(stripId.toString(), ledId.toString())

                if (actor != null && payload != valuesHistoric.get(identifier)) {
                    valuesHistoric.set(identifier, payload)

                    val mqttEvents = ArrayList<MQTTEvent>()
                    val lowerLevelTopic = SentientProperties.MQTT.Topic.LED
                    val lowerLevelPayload = SingleLEDPayload(stripId.toString(), ledId.toString(), warmWhite.toString(), coldWhite.toString(), amber.toString())
                    val mqttEvent = MQTTEvent(lowerLevelTopic, Gson().toJson(lowerLevelPayload), Date())

                    mqttEvents.add(mqttEvent)

                    // Call SerialSetLEDAsyncTask
                    SyncTaskExecutor().execute(MQTTPublishAsyncTask(mqttEvents))
                }
            } catch (jse: JsonSyntaxException) {
                log.severe("$SentientProperties.Color.ERROR$jse$SentientProperties.Color.RESET")
            }
        }
    }
}
