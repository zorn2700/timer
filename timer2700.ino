#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <UniversalTelegramBot.h>
#include <time.h>

// ========== بيانات الواي فاي ==========
const char* ssid = "name ssid";
const char* password = "pass";

// ========== بيانات تلغرام ==========
#define BOT_TOKEN "000000000"   // ضع توكن البوت هنا
#define CHAT_ID "000000000"                   // ضع Chat ID الخاص بك هنا

WiFiClientSecure client;
UniversalTelegramBot bot(BOT_TOKEN, client);

// ========== إعدادات المنفذ والجدول ==========
const int relayPin = 18;   // GPIO18 (D5)
bool manualOverride = false;
bool manualState = false;

// ========== وظائف التحكم ==========
bool shouldRelayBeOn() {
  if (manualOverride) return manualState;

  struct tm timeinfo;
  if (!getLocalTime(&timeinfo)) return false;
  return (timeinfo.tm_hour >= 19);  // من 7 مساءً حتى منتصف الليل
}

void updateRelay() {
  bool on = shouldRelayBeOn();
  // معظم وحدات الريليه تنشط عند LOW، وإذا كانت تنشط عند HIGH اقلب السطر
  digitalWrite(relayPin, on ? LOW : HIGH);
}

// ========== معالجة رسائل تلغرام ==========
void handleTelegramMessages(int numNewMessages) {
  for (int i = 0; i < numNewMessages; i++) {
    String chat_id = bot.messages[i].chat_id;
    String text = bot.messages[i].text;
    text.trim();
    // تجاهل الرسائل من غير المخوّل (إن أردت السماح للجميع، أزل الشرط)
    if (chat_id != CHAT_ID) {
      bot.sendMessage(chat_id, "غير مصرح لك");
      continue;
    }

    if (text == "/on") {
      manualOverride = true;
      manualState = true;
      updateRelay();
      bot.sendMessage(chat_id, "✅ تم تشغيل اللوحة يدوياً");
    }
    else if (text == "/off") {
      manualOverride = true;
      manualState = false;
      updateRelay();
      bot.sendMessage(chat_id, "✅ تم إطفاء اللوحة يدوياً");
    }
    else if (text == "/auto") {
      manualOverride = false;
      updateRelay();
      bot.sendMessage(chat_id, "✅ عودة إلى الجدول التلقائي (7 مساءً - 12 ليلاً)");
    }
    else if (text == "/status") {
      String state = shouldRelayBeOn() ? "قيد التشغيل 💡" : "مطفأة 🌙";
      String mode = manualOverride ? "يدوي" : "تلقائي";
      struct tm timeinfo;
      String timeStr = "غير متاح";
      if (getLocalTime(&timeinfo)) {
        char buf[6];
        sprintf(buf, "%02d:%02d", timeinfo.tm_hour, timeinfo.tm_min);
        timeStr = buf;
      }
      String msg = "الوقت: " + timeStr + "\nالحالة: " + state + "\nالوضع: " + mode;
      bot.sendMessage(chat_id, msg);
    }
    else {
      bot.sendMessage(chat_id, "أوامر متاحة:\n/on - تشغيل\n/off - إطفاء\n/auto - عودة للجدول\n/status - الحالة");
    }
  }
}

// ========== Setup ==========
void setup() {
  Serial.begin(115200);
  pinMode(relayPin, OUTPUT);
  digitalWrite(relayPin, HIGH); // إطفاء أولي (إذا كان التنشيط عند LOW)

  // الاتصال بالواي فاي
  WiFi.begin(ssid, password);
  Serial.print("جاري الاتصال");
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\n✅ متصل بالواي فاي");

  // ضبط الوقت (UTC+4)
  configTime(4 * 3600, 0, "pool.ntp.org", "time.nist.gov");
  struct tm timeinfo;
  while (!getLocalTime(&timeinfo)) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("✅ تم ضبط الوقت");

  // إعداد تلغرام (تجاوز شهادة SSL للتبسيط)
  client.setInsecure();
  bot.longPoll = 60;  // مهلة طويلة للـ long polling

  // إرسال رسالة بدء التشغيل للمخوّل
  bot.sendMessage(CHAT_ID, "🤖 بوت اللوحة الإعلانية جاهز\n"
  "الأوامر:\n/on - تشغيل\n/off - إطفاء\n/auto - الجدول\n/status - حالة");
}

// ========== Loop ==========
void loop() {
  // التحقق من رسائل تلغرام كل ثانية (تقريباً)
  int numNewMessages = bot.getUpdates(bot.last_message_received + 1);

  while (numNewMessages) {
    handleTelegramMessages(numNewMessages);
    numNewMessages = bot.getUpdates(bot.last_message_received + 1);
  }

  // تحديث المرحل كل دقيقة
  static unsigned long lastCheck = 0;
  if (millis() - lastCheck >= 60000 || lastCheck == 0) {
    lastCheck = millis();
    updateRelay();
    Serial.println(shouldRelayBeOn() ? "ON" : "OFF");
  }
}
