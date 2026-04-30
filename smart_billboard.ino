/*
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║        لوحة إعلانية ذكية – Smart Billboard v6.0 ULTIMATE                   ║
 * ║                 ESP32 Professional – Self-Contained Edition                 ║
 * ╚══════════════════════════════════════════════════════════════════════════════╝
 *
 * تم تصحيح جميع الأخطاء – جاهز للترجمة
 */

// ═══════════════════════════════════════════════════════
//  مفاتيح التفعيل
// ═══════════════════════════════════════════════════════
//#define USE_BLE
//#define USE_WEBSOCKET
//#define USE_MDNS
#define USE_CAPTIVE_PORTAL
//#define USE_ESPNOW
#define USE_LITTLEFS
//#define USE_SELF_UPDATE
#define USE_ADVANCED_SCHEDULE
#define USE_RATE_LIMIT
#define USE_AUDIT_LOG
// #define USE_DHT_SENSOR

// ═══════════════════════════════════════════════════════
//  المكتبات
// ═══════════════════════════════════════════════════════
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <UniversalTelegramBot.h>
#include <ArduinoJson.h>
#include <time.h>
#include <WebServer.h>
#include <ElegantOTA.h>
#include <Wire.h>
#include <U8g2lib.h>
#include <Preferences.h>
#include <esp_task_wdt.h>
#include <vector>
#include <map>
#include <PubSubClient.h>
#include <DNSServer.h>
#include <MD5Builder.h>

#ifdef USE_LITTLEFS
#include <LittleFS.h>
#define FS_IMPL LittleFS
#endif

#ifdef USE_BLE
#include <BLEDevice.h>
#include <BLEServer.h>
#include <BLEUtils.h>
#include <BLE2902.h>
#define BLE_SERVICE_UUID     "4fafc201-1fb5-459e-8fcc-c5c9c331914b"
#define BLE_CHAR_CMD_UUID    "beb5483e-36e1-4688-b7f5-ea07361b26a8"
#define BLE_CHAR_STATUS_UUID "beb5483f-36e1-4688-b7f5-ea07361b26a8"
BLEServer*         pBLEServer  = nullptr;
BLECharacteristic* pCmdChar    = nullptr;
BLECharacteristic* pStatusChar = nullptr;
bool               bleConnected = false;
#endif

#ifdef USE_WEBSOCKET
#include <WebSocketsServer.h>
WebSocketsServer wsServer(81);
#endif

#ifdef USE_MDNS
#include <ESPmDNS.h>
#endif

#ifdef USE_ESPNOW
#include <esp_now.h>
uint8_t ESPNOW_PEER_MAC[] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
typedef struct { uint8_t cmd; uint32_t ts; float temp; float hum; bool relay; } ESPNowMsg;
ESPNowMsg espNowTx, espNowRx;
bool espNowReady = false;
void espNowBroadcast();  // إعلان مسبق
#endif

#ifdef USE_DHT_SENSOR
#include <DHT.h>
DHT dht(14, DHT22);
float temperature=0, humidity=0;
bool sensorOK=false, tempAlertSent=false;
#endif

// ═══════════════════════════════════════════════════════
//  إعدادات الشبكة
// ═══════════════════════════════════════════════════════
struct WiFiCred { const char* ssid; const char* pass; };
WiFiCred WIFI_NETS[3] = {
  { "Valencia main office", "valencia@2023" },
  { "2700",                 "11199777"       },
  { "Valencia Account 4G",  "VM@123456$"     },
};

// ═══════════════════════════════════════════════════════
//  الإعدادات الثابتة
// ═══════════════════════════════════════════════════════
namespace Config {
  constexpr char BOT_TOKEN[]    = "8459387658:AAHb3DJnMUCXTxgKcp6XT7JBOwfq3yqnPrc";
  const String   ALLOWED_CHATS[]= {"-1003552031590"};
  constexpr int  ALLOWED_COUNT  = 1;
  constexpr char OTA_USER[]     = "admin";
  constexpr char OTA_PASS[]     = "billboard2024";
  constexpr int  WEB_PORT       = 8080;
  constexpr int  WS_PORT        = 81;
  constexpr char MDNS_NAME[]    = "billboard";
  constexpr char BLE_NAME[]     = "SmartBillboard";
  constexpr char CAPTIVE_SSID[] = "Billboard-Setup";
  constexpr char CAPTIVE_PASS[] = "12345678";
  constexpr char GH_USER[]      = "";
  constexpr char GH_REPO[]      = "";
  constexpr char GH_BRANCH[]    = "main";
  constexpr int  RELAY_PIN      = 18;
  constexpr int  STATUS_LED     = 2;
  constexpr int  BUTTON_PIN     = 13;
  constexpr int  DEFAULT_START  = 19;
  constexpr int  DEFAULT_END    = 0;
  constexpr int  DEFAULT_TZ     = 4;
  constexpr int  BOT_POLL_MS    = 2000;
  constexpr int  DISPLAY_UPD_S  = 20;
  constexpr int  RELAY_CHK_S    = 60;
  constexpr int  WDT_S          = 120;
  constexpr int  STATS_SAVE_S   = 300;
  constexpr int  WS_PUSH_MS     = 1000;
  constexpr char MQTT_SRV[]     = "";
  constexpr int  MQTT_PORT      = 1883;
  constexpr char MQTT_USER[]    = "";
  constexpr char MQTT_PASS[]    = "";
  constexpr char MQTT_CID[]     = "SmartBillboard";
  constexpr char MQTT_CMD[]     = "billboard/cmd";
  constexpr char MQTT_STATE[]   = "billboard/state";
  constexpr int  DIM_START      = 23;
  constexpr int  DIM_END        = 6;
  constexpr char DASHBOARD_URL[] = "https://raw.githubusercontent.com/zorn2700/timer/main/dashboard.html";
  constexpr int  CONTRAST_NIGHT = 50;
  constexpr int  CONTRAST_DAY   = 255;
  constexpr float TEMP_ALERT_H  = 45.0;
  constexpr int  RATE_LIMIT_REQ = 30;
  constexpr int  RATE_WINDOW_MS = 60000;
}

// ═══════════════════════════════════════════════════════
//  نظام الأمان والصلاحيات
// ═══════════════════════════════════════════════════════
enum UserRole { ROLE_VIEWER=0, ROLE_OPERATOR=1, ROLE_ADMIN=2 };

struct UserRecord {
  String username;
  String passHash;
  UserRole role;
  bool enabled;
};

struct AuthToken {
  String token;
  String username;
  UserRole role;
  unsigned long expiry;
  String clientIP;
};

#ifdef USE_RATE_LIMIT
struct RateEntry { int count; unsigned long window; };
std::map<String,RateEntry> rateLimitMap;
#endif

std::vector<UserRecord> users;
std::vector<AuthToken>  tokens;
std::vector<String>     ipWhitelist;

String md5Hash(const String& input) {
  MD5Builder md5; md5.begin(); md5.add(input); md5.calculate();
  return md5.toString();
}

String generateToken() {
  char buf[33];
  for (int i=0;i<32;i++) buf[i]="0123456789abcdef"[random(16)];
  buf[32]=0; return String(buf);
}

String createAuthToken(const String& user, UserRole role, const String& ip) {
  tokens.erase(std::remove_if(tokens.begin(),tokens.end(),
                              [&](const AuthToken& t){ return t.username==user; }),tokens.end());
  AuthToken tk;
  tk.token    = generateToken();
  tk.username = user;
  tk.role     = role;
  tk.expiry   = millis() + 24UL*3600*1000;
  tk.clientIP = ip;
  tokens.push_back(tk);
  return tk.token;
}

UserRole* validateToken(const String& token, const String& ip="") {
  for (auto& t : tokens) {
    if (t.token==token && millis()<t.expiry) {
      if (ip.length()>0 && t.clientIP!=ip) continue;
      return &t.role;
    }
  }
  return nullptr;
}

bool checkRateLimit(const String& ip) {
  #ifdef USE_RATE_LIMIT
  auto& e = rateLimitMap[ip];
  unsigned long now = millis();
  if (now - e.window > Config::RATE_WINDOW_MS) { e.window=now; e.count=0; }
  if (++e.count > Config::RATE_LIMIT_REQ) return false;
  #endif
  return true;
}

// ═══════════════════════════════════════════════════════
//  نظام الجدول المتقدم
// ═══════════════════════════════════════════════════════
#ifdef USE_ADVANCED_SCHEDULE

struct ScheduleRule {
  String   name;
  uint8_t  type;
  uint8_t  priority;
  bool     enabled;
  bool     forceOn;
  uint8_t  startH, startM;
  uint8_t  endH,   endM;
  uint8_t  weekDays;
  uint8_t  month, day;
  uint8_t  seasonStart, seasonEnd;
  time_t   expiryDate;
  uint32_t triggerCount;
};

std::vector<ScheduleRule> scheduleRules;

void addDefaultRules() {
  ScheduleRule r;
  r.name="Default Evening"; r.type=0; r.priority=1; r.enabled=true; r.forceOn=true;
  r.startH=19; r.startM=0; r.endH=0; r.endM=0; r.weekDays=0x7F;
  r.month=0; r.day=0; r.seasonStart=0; r.seasonEnd=0; r.expiryDate=0; r.triggerCount=0;
  scheduleRules.push_back(r);
}

bool evalAdvancedSchedule() {
  struct tm t; if (!getLocalTime(&t)) return false;
  int h=t.tm_hour, m=t.tm_min, wday=t.tm_wday;
  int nowMin = h*60+m;

  std::vector<ScheduleRule*> active;
  for (auto& r : scheduleRules) {
    if (!r.enabled) continue;
    if (r.expiryDate>0 && time(nullptr)>r.expiryDate) continue;
    bool dayMatch = false;
    if (r.type==0) dayMatch=true;
    else if (r.type==1) dayMatch=(r.weekDays & (1<<wday))!=0;
    else if (r.type==2) dayMatch=(t.tm_mon+1==r.month && t.tm_mday==r.day);
    else if (r.type==3) {
      int cm=t.tm_mon+1;
      dayMatch=(r.seasonStart<=r.seasonEnd)?(cm>=r.seasonStart&&cm<=r.seasonEnd):(cm>=r.seasonStart||cm<=r.seasonEnd);
    }
    if (!dayMatch) continue;
    int sMin=r.startH*60+r.startM, eMin=r.endH*60+r.endM;
    bool timeMatch;
    if (eMin==0) timeMatch=(nowMin>=sMin);
    else if (sMin<eMin) timeMatch=(nowMin>=sMin&&nowMin<eMin);
    else timeMatch=(nowMin>=sMin||nowMin<eMin);
    if (timeMatch) active.push_back(&r);
  }
  if (active.empty()) return false;
  ScheduleRule* best=active[0];
  for (auto* r:active) if (r->priority>best->priority) best=r;
  best->triggerCount++;
  return best->forceOn;
}

#endif

// ═══════════════════════════════════════════════════════
//  Audit Log
// ═══════════════════════════════════════════════════════
#define AUDIT_SIZE 200
struct AuditEntry { unsigned long ts; String user; String action; String ip; bool success; };
AuditEntry auditLog[AUDIT_SIZE];
int auditIdx=0;

void audit(const String& user, const String& action, const String& ip="", bool ok=true) {
  auditLog[auditIdx%AUDIT_SIZE] = {(unsigned long)millis(), user, action, ip, ok};
  auditIdx++;
  #ifdef USE_AUDIT_LOG
  File f = FS_IMPL.open("/audit.log","a");
  if (f) {
    char buf[128];
    snprintf(buf,sizeof(buf),"%lu|%s|%s|%s|%d\n", millis(),user.c_str(),action.c_str(),ip.c_str(),(int)ok);
    f.print(buf); f.close();
  }
  #endif
}

// ═══════════════════════════════════════════════════════
//  الكائنات العامة
// ═══════════════════════════════════════════════════════
WiFiClientSecure secureClient;
WiFiClient       plainClient;
UniversalTelegramBot bot(Config::BOT_TOKEN, secureClient);
WebServer        webServer(Config::WEB_PORT);
Preferences      prefs;
U8G2_SSD1306_128X64_NONAME_1_HW_I2C display(U8G2_R0, U8X8_PIN_NONE);
WiFiClient       mqttWiFiClient;
PubSubClient     mqtt(mqttWiFiClient);
DNSServer        dnsServer;

// ═══════════════════════════════════════════════════════
//  سجل الأحداث الرئيسي
// ═══════════════════════════════════════════════════════
#define LOG_SIZE 50
struct LogEntry { uint32_t ts; String msg; };
LogEntry eventLog[LOG_SIZE];
int logIdx=0;

void addLog(const String& msg) {
  eventLog[logIdx%LOG_SIZE]={millis(),msg};
  logIdx++;
  Serial.println("[LOG] "+msg);
}

// ═══════════════════════════════════════════════════════
//  إحصائيات التشغيل
// ═══════════════════════════════════════════════════════
struct Stats {
  unsigned long totalOnSec, todayOnSec, lastOnTime;
  uint32_t powerCycles, dailyCycles;
  int lastLogDay;
  float estimatedKWh;
  float weeklyHours[7];
  uint32_t weeklyCycles[7];
  int weekDataStart;
} stats;
float LOAD_WATTS = 200.0;

void recordDailyStats() {
  int d = stats.weekDataStart % 7;
  stats.weeklyHours[d]  = stats.todayOnSec/3600.0;
  stats.weeklyCycles[d] = stats.dailyCycles;
  stats.weekDataStart   = (stats.weekDataStart+1)%7;
}

// ═══════════════════════════════════════════════════════
//  حالة النظام
// ═══════════════════════════════════════════════════════
struct SystemState {
  uint8_t  startHour, endHour;
  int32_t  tzOffset;
  bool     useWeekSchedule, useAdvancedSchedule;
  bool     manualOverride, manualState, relayOn;
  bool     emergencyMode, controlLocked;
  bool     captivePortalActive;
  bool     wifiConnected, otaInProgress;
  int8_t   connectedNetIdx;
  unsigned long bootTime;
  unsigned long lastBotCheck, lastRelayCheck;
  unsigned long lastDisplayUpd, lastSensorRead;
  unsigned long lastMqttPub, lastStatsSave;
  unsigned long lastWsPush;
  uint8_t  displayPage;
  uint8_t  oledContrast;
  uint32_t msgCount;
  unsigned long timerEnd;
  bool     timerActive;
  String   fwVersion;
  String   lastGhCheck;
  bool     updateAvailable;
  String   updateVersion;
  float    peakOnHours;
} sys;

// ─── إعدادات MQTT وتعتيم ──────────────────────────────
String mqttServer=Config::MQTT_SRV, mqttTopicCmd=Config::MQTT_CMD, mqttTopicState=Config::MQTT_STATE;
String mqttUser=Config::MQTT_USER, mqttPassStr=Config::MQTT_PASS, mqttClientId=Config::MQTT_CID;
int    mqttPort=Config::MQTT_PORT;
int    nightDimStart=Config::DIM_START, nightDimEnd=Config::DIM_END;
int    nightContrast=Config::CONTRAST_NIGHT, dayContrast=Config::CONTRAST_DAY;

String adminPassword="";
std::vector<String> authenticatedChats;

// ═══════════════════════════════════════════════════════
//  إعلانات مسبقة للدوال التي تُستعمل قبل تعريفها
// ═══════════════════════════════════════════════════════
String getTimeStr();
void setRelay(bool on);
bool shouldBeOn();
String buildStatusJson();
bool fetchDashboardHTML();
void serveDashboard();
void updateDisplay();
void saveConfigJSON();
void saveScheduleJSON();
void saveUsersJSON();
void loadConfigJSON();
void loadScheduleJSON();
void loadUsersJSON();
bool initFS();
String buildFullBackup();
void updateRelay();

#ifdef USE_BLE
void updateBLEStatus();
#endif

#ifdef USE_WEBSOCKET
void wsBroadcast();
#endif

// ═══════════════════════════════════════════════════════
//  LittleFS
// ═══════════════════════════════════════════════════════
#ifdef USE_LITTLEFS

bool initFS() {
  if (!FS_IMPL.begin(true)) { addLog("LittleFS: فشل التهيئة"); return false; }
  addLog("LittleFS: " + String(FS_IMPL.totalBytes()/1024) + "KB إجمالي، " + String(FS_IMPL.usedBytes()/1024) + "KB مستخدم");
  return true;
}

void saveConfigJSON() {
  DynamicJsonDocument doc(4096);
  doc["startHour"]  = sys.startHour;
  doc["endHour"]    = sys.endHour;
  doc["tzOffset"]   = sys.tzOffset/3600;
  doc["weekSched"]  = sys.useWeekSchedule;
  doc["advSched"]   = sys.useAdvancedSchedule;
  doc["loadWatts"]  = LOAD_WATTS;
  doc["mqttSrv"]    = mqttServer;
  doc["mqttPort"]   = mqttPort;
  doc["mqttUser"]   = mqttUser;
  doc["mqttPass"]   = mqttPassStr;
  doc["mqttCmd"]    = mqttTopicCmd;
  doc["mqttState"]  = mqttTopicState;
  doc["dimS"]       = nightDimStart;
  doc["dimE"]       = nightDimEnd;
  doc["cntN"]       = nightContrast;
  doc["cntD"]       = dayContrast;
  doc["adminPass"]  = adminPassword;
  doc["fwVersion"]  = sys.fwVersion;
  doc["totalOnSec"] = stats.totalOnSec;
  doc["cycles"]     = stats.powerCycles;
  doc["kWh"]        = stats.estimatedKWh;
  doc["logDay"]     = stats.lastLogDay;
  doc["peakOnH"]    = sys.peakOnHours;
  JsonArray wh = doc.createNestedArray("weeklyHours");
  JsonArray wc = doc.createNestedArray("weeklyCycles");
  for (int i=0;i<7;i++) { wh.add(stats.weeklyHours[i]); wc.add(stats.weeklyCycles[i]); }
  File f = FS_IMPL.open("/config.json","w");
  if (f) { serializeJson(doc,f); f.close(); }
}

void loadConfigJSON() {
  if (!FS_IMPL.exists("/config.json")) return;
  File f = FS_IMPL.open("/config.json","r");
  if (!f) return;
  DynamicJsonDocument doc(4096);
  if (deserializeJson(doc,f)) { f.close(); return; }
  f.close();
  sys.startHour = doc["startHour"] | Config::DEFAULT_START;
  sys.endHour = doc["endHour"] | Config::DEFAULT_END;
  sys.tzOffset = (int)(doc["tzOffset"] | Config::DEFAULT_TZ) * 3600;
  sys.useWeekSchedule = doc["weekSched"] | false;
  sys.useAdvancedSchedule = doc["advSched"] | false;
  LOAD_WATTS = doc["loadWatts"] | 200.0f;
  mqttServer = doc["mqttSrv"].as<String>();
  mqttPort = doc["mqttPort"] | 1883;
  mqttUser = doc["mqttUser"].as<String>();
  mqttPassStr = doc["mqttPass"].as<String>();
  mqttTopicCmd = doc["mqttCmd"].as<String>();
  mqttTopicState = doc["mqttState"].as<String>();
  nightDimStart = doc["dimS"] | Config::DIM_START;
  nightDimEnd = doc["dimE"] | Config::DIM_END;
  nightContrast = doc["cntN"] | Config::CONTRAST_NIGHT;
  dayContrast = doc["cntD"] | Config::CONTRAST_DAY;
  adminPassword = doc["adminPass"].as<String>();
  sys.fwVersion = doc["fwVersion"] | "v6.0";
  stats.totalOnSec = doc["totalOnSec"] | 0;
  stats.powerCycles = doc["cycles"] | 0;
  stats.estimatedKWh = doc["kWh"] | 0.0f;
  stats.lastLogDay = doc["logDay"] | -1;
  sys.peakOnHours = doc["peakOnH"] | 0.0f;
  if (doc.containsKey("weeklyHours")) {
    for (int i=0;i<7;i++) stats.weeklyHours[i]=doc["weeklyHours"][i]|0.0f;
  }
  if (doc.containsKey("weeklyCycles")) {
    for (int i=0;i<7;i++) stats.weeklyCycles[i]=doc["weeklyCycles"][i]|0;
  }
}

void saveScheduleJSON() {
  #ifdef USE_ADVANCED_SCHEDULE
  DynamicJsonDocument doc(8192);
  JsonArray arr = doc.createNestedArray("rules");
  for (auto& r:scheduleRules) {
    JsonObject o=arr.createNestedObject();
    o["name"]=r.name; o["type"]=r.type; o["priority"]=r.priority;
    o["enabled"]=r.enabled; o["forceOn"]=r.forceOn;
    o["startH"]=r.startH; o["startM"]=r.startM;
    o["endH"]=r.endH; o["endM"]=r.endM;
    o["weekDays"]=r.weekDays;
    o["month"]=r.month; o["day"]=r.day;
    o["seasonStart"]=r.seasonStart; o["seasonEnd"]=r.seasonEnd;
    o["expiry"]=(long long)r.expiryDate;
    o["triggers"]=r.triggerCount;
  }
  JsonArray ws=doc.createNestedArray("weekly");
  for (int d=0;d<7;d++) {
    JsonObject wo=ws.createNestedObject();
    wo["enabled"]=true; wo["startH"]=sys.startHour; wo["endH"]=sys.endHour;
  }
  File f=FS_IMPL.open("/schedule.json","w");
  if (f){serializeJson(doc,f);f.close();}
  #endif
}

void loadScheduleJSON() {
  #ifdef USE_ADVANCED_SCHEDULE
  if (!FS_IMPL.exists("/schedule.json")) { addDefaultRules(); return; }
  File f=FS_IMPL.open("/schedule.json","r");
  if (!f){addDefaultRules();return;}
  DynamicJsonDocument doc(8192);
  if (deserializeJson(doc,f)){f.close();addDefaultRules();return;}
  f.close();
  scheduleRules.clear();
  for (JsonObject o:doc["rules"].as<JsonArray>()) {
    ScheduleRule r;
    r.name = o["name"].as<String>();
    r.type = o["type"]|0;
    r.priority = o["priority"]|1;
    r.enabled = o["enabled"]|true;
    r.forceOn = o["forceOn"]|true;
    r.startH = o["startH"]|19; r.startM=o["startM"]|0;
    r.endH = o["endH"]|0;   r.endM =o["endM"]  |0;
    r.weekDays = o["weekDays"]|0x7F;
    r.month = o["month"]|0; r.day=o["day"]|0;
    r.seasonStart = o["seasonStart"]|0; r.seasonEnd=o["seasonEnd"]|0;
    r.expiryDate = (time_t)(long long)o["expiry"]|0;
    r.triggerCount = o["triggers"]|0;
    scheduleRules.push_back(r);
  }
  #endif
}

void saveUsersJSON() {
  DynamicJsonDocument doc(4096);
  JsonArray arr=doc.createNestedArray("users");
  for (auto& u:users) {
    JsonObject o=arr.createNestedObject();
    o["username"]=u.username; o["passHash"]=u.passHash;
    o["role"]=(int)u.role; o["enabled"]=u.enabled;
  }
  JsonArray wl=doc.createNestedArray("whitelist");
  for (auto& ip:ipWhitelist) wl.add(ip);
  File f=FS_IMPL.open("/users.json","w");
  if (f){serializeJson(doc,f);f.close();}
}

void loadUsersJSON() {
  if (!FS_IMPL.exists("/users.json")) {
    users.push_back({"admin", md5Hash("billboard2024billboard"), ROLE_ADMIN, true});
    users.push_back({"operator", md5Hash(String("oper123") + "billboard"), ROLE_OPERATOR, true});
    saveUsersJSON(); return;
  }
  File f=FS_IMPL.open("/users.json","r");
  if (!f) return;
  DynamicJsonDocument doc(4096);
  if (deserializeJson(doc,f)){f.close();return;}
  f.close();
  users.clear();
  for (JsonObject o:doc["users"].as<JsonArray>())
    users.push_back({o["username"],o["passHash"],(UserRole)(int)o["role"],o["enabled"]|true});
  for (String ip:doc["whitelist"].as<JsonArray>()) ipWhitelist.push_back(ip);
}

String buildFullBackup() {
  DynamicJsonDocument doc(16384);
  doc["version"]   = "6.0";
  doc["timestamp"] = time(nullptr);
  if (FS_IMPL.exists("/config.json")) {
    File f=FS_IMPL.open("/config.json","r");
    DynamicJsonDocument cfg(4096); deserializeJson(cfg,f); f.close();
    doc["config"]=cfg;
  }
  if (FS_IMPL.exists("/schedule.json")) {
    File f=FS_IMPL.open("/schedule.json","r");
    DynamicJsonDocument sch(8192); deserializeJson(sch,f); f.close();
    doc["schedule"]=sch;
  }
  if (FS_IMPL.exists("/users.json")) {
    File f=FS_IMPL.open("/users.json","r");
    DynamicJsonDocument usr(4096); deserializeJson(usr,f); f.close();
    doc["users"]=usr;
  }
  String out; serializeJson(doc,out); return out;
}

bool restoreFromBackup(const String& json) {
  DynamicJsonDocument doc(16384);
  if (deserializeJson(doc,json)) return false;
  if (!doc.containsKey("version")) return false;
  if (doc.containsKey("config")) {
    File f=FS_IMPL.open("/config.json","w");
    serializeJson(doc["config"],f); f.close();
  }
  if (doc.containsKey("schedule")) {
    File f=FS_IMPL.open("/schedule.json","w");
    serializeJson(doc["schedule"],f); f.close();
  }
  if (doc.containsKey("users")) {
    File f=FS_IMPL.open("/users.json","w");
    serializeJson(doc["users"],f); f.close();
  }
  return true;
}

String readFile(const String& path) {
  if (!FS_IMPL.exists(path)) return "";
  File f=FS_IMPL.open(path,"r");
  if (!f) return "";
  String s=f.readString(); f.close(); return s;
}

bool writeFile(const String& path, const String& content) {
  File f=FS_IMPL.open(path,"w");
  if (!f) return false;
  f.print(content); f.close(); return true;
}

String getFSInfo() {
  DynamicJsonDocument doc(1024);
  doc["total"] = FS_IMPL.totalBytes();
  doc["used"]  = FS_IMPL.usedBytes();
  doc["free"]  = FS_IMPL.totalBytes()-FS_IMPL.usedBytes();
  JsonArray files=doc.createNestedArray("files");
  File root=FS_IMPL.open("/");
  File file=root.openNextFile();
  while (file) {
    JsonObject fo=files.createNestedObject();
    fo["name"]=String(file.name()); fo["size"]=file.size();
    file=root.openNextFile();
  }
  String out; serializeJson(doc,out); return out;
}

#endif

// ═══════════════════════════════════════════════════════
//  Self-Update من GitHub
// ═══════════════════════════════════════════════════════
#ifdef USE_SELF_UPDATE
void checkGitHubUpdate() {
  if (strlen(Config::GH_USER)==0) return;
  HTTPClient http;
  String url = "https://api.github.com/repos/" + String(Config::GH_USER) + "/" + String(Config::GH_REPO) + "/releases/latest";
  http.begin(url);
  http.addHeader("User-Agent","SmartBillboard/6.0");
  int code=http.GET();
  if (code==200) {
    DynamicJsonDocument doc(4096);
    deserializeJson(doc,http.getStream());
    String tag=doc["tag_name"].as<String>();
    sys.lastGhCheck=getTimeStr();
    if (tag!="" && tag!=sys.fwVersion) {
      sys.updateAvailable=true;
      sys.updateVersion=tag;
      addLog("تحديث متاح: "+tag);
      bot.sendMessage(Config::ALLOWED_CHATS[0], "🔄 تحديث firmware متاح!\n" + sys.fwVersion + " → " + tag + "\nأرسل /selfupdate للتحديث","Markdown");
    } else sys.updateAvailable=false;
  }
  http.end();
}

void performSelfUpdate(const String& binUrl) {
  addLog("Self-Update: " + binUrl);
  HTTPClient http;
  http.begin(binUrl);
  int code=http.GET();
  if (code==200) {
    int total=http.getSize();
    WiFiClient* stream=http.getStreamPtr();
    if (Update.begin(total)) {
      size_t written=Update.writeStream(*stream);
      if (written==total && Update.end(true)) {
        addLog("Self-Update: ناجح → إعادة التشغيل");
        bot.sendMessage(Config::ALLOWED_CHATS[0],"✅ تحديث firmware ناجح! يعيد التشغيل...");
        delay(2000); ESP.restart();
      }
    }
  }
  http.end();
  addLog("Self-Update: فشل");
}
#endif

// ═══════════════════════════════════════════════════════
//  بناء JSON الحالة الكاملة
// ═══════════════════════════════════════════════════════
String buildStatusJson() {
  DynamicJsonDocument doc(2048);
  doc["relay"]       = sys.relayOn;
  doc["manual"]      = sys.manualOverride;
  doc["emergency"]   = sys.emergencyMode;
  doc["locked"]      = sys.controlLocked;
  doc["timer"]       = sys.timerActive;
  if (sys.timerActive && sys.timerEnd>millis()) doc["timerLeft"] = (sys.timerEnd-millis())/60000;
  doc["startHour"]   = sys.startHour;
  doc["endHour"]     = sys.endHour;
  doc["tzOffset"]    = sys.tzOffset/3600;
  doc["weekSched"]   = sys.useWeekSchedule;
  doc["advSched"]    = sys.useAdvancedSchedule;
  doc["ip"]          = WiFi.localIP().toString();
  doc["rssi"]        = WiFi.RSSI();
  doc["ssid"]        = WiFi.SSID();
  doc["uptime"]      = (millis()-sys.bootTime)/1000;
  doc["msgCount"]    = sys.msgCount;
  doc["totalOnH"]    = stats.totalOnSec/3600.0;
  doc["todayOnH"]    = stats.todayOnSec/3600.0;
  doc["kWh"]         = stats.estimatedKWh;
  doc["cycles"]      = stats.powerCycles;
  doc["loadW"]       = LOAD_WATTS;
  doc["freeHeap"]    = ESP.getFreeHeap();
  doc["displayPage"] = sys.displayPage;
  doc["fwVersion"]   = sys.fwVersion;
  doc["updateAvail"] = sys.updateAvailable;
  doc["updateVer"]   = sys.updateVersion;
  doc["peakOnH"]     = sys.peakOnHours;
  #ifdef USE_DHT_SENSOR
  doc["temp"]        = sensorOK?temperature:-999;
  doc["hum"]         = sensorOK?humidity:-1;
  #endif
  #ifdef USE_BLE
  doc["ble"]         = bleConnected;
  #endif
  JsonArray wh=doc.createNestedArray("weeklyHours");
  JsonArray wc=doc.createNestedArray("weeklyCycles");
  for (int i=0;i<7;i++){wh.add(stats.weeklyHours[i]);wc.add(stats.weeklyCycles[i]);}
  struct tm t;
  if (getLocalTime(&t)) {
    char tb[6]; sprintf(tb,"%02d:%02d",t.tm_hour,t.tm_min);
    doc["time"]=tb;
    char db[11]; sprintf(db,"%04d-%02d-%02d",t.tm_year+1900,t.tm_mon+1,t.tm_mday);
    doc["date"]=db;
    doc["weekDay"]=t.tm_wday;
  }
  #ifdef USE_LITTLEFS
  doc["fsUsed"]  = FS_IMPL.usedBytes();
  doc["fsTotal"] = FS_IMPL.totalBytes();
  #endif
  String out; serializeJson(doc,out); return out;
}

// ═══════════════════════════════════════════════════════
//  BLE
// ═══════════════════════════════════════════════════════
#ifdef USE_BLE
class BLEServerCB:public BLEServerCallbacks{
  void onConnect(BLEServer*s)override{bleConnected=true;addLog("BLE: متصل");updateBLEStatus();}
  void onDisconnect(BLEServer*s)override{bleConnected=false;addLog("BLE: انفصل");s->getAdvertising()->start();}
};
class BLECmdCB:public BLECharacteristicCallbacks{
  void onWrite(BLECharacteristic*c)override{
    String cmd=c->getValue().c_str(); cmd.trim(); cmd.toLowerCase();
    addLog("BLE CMD: "+cmd);
    if(cmd=="on"){sys.manualOverride=true;sys.manualState=true;setRelay(true);}
    else if(cmd=="off"){sys.manualOverride=true;sys.manualState=false;setRelay(false);}
    else if(cmd=="auto"){sys.manualOverride=false;}
    else if(cmd=="toggle"){sys.manualOverride=true;sys.manualState=!sys.relayOn;setRelay(sys.manualState);}
    else if(cmd=="status"){updateBLEStatus();}
    else if(cmd.startsWith("timer:")){
      int m=cmd.substring(6).toInt();
      if(m>0){sys.timerEnd=millis()+(unsigned long)m*60000;sys.timerActive=true;
        sys.manualOverride=true;sys.manualState=true;setRelay(true);}
    }
    audit("ble","cmd:"+cmd,"BLE");
    updateBLEStatus();
  }
};
void initBLE(){
  BLEDevice::init(Config::BLE_NAME);
  pBLEServer=BLEDevice::createServer();
  pBLEServer->setCallbacks(new BLEServerCB());
  BLEService* svc=pBLEServer->createService(BLE_SERVICE_UUID);
  pCmdChar=svc->createCharacteristic(BLE_CHAR_CMD_UUID, BLECharacteristic::PROPERTY_WRITE|BLECharacteristic::PROPERTY_WRITE_NR);
  pCmdChar->setCallbacks(new BLECmdCB());
  pStatusChar=svc->createCharacteristic(BLE_CHAR_STATUS_UUID, BLECharacteristic::PROPERTY_READ|BLECharacteristic::PROPERTY_NOTIFY);
  pStatusChar->addDescriptor(new BLE2902());
  svc->start();
  BLEAdvertising*adv=BLEDevice::getAdvertising();
  adv->addServiceUUID(BLE_SERVICE_UUID); adv->setScanResponse(true); adv->setMinPreferred(0x06);
  BLEDevice::startAdvertising();
  addLog("BLE: "+String(Config::BLE_NAME));
}
void updateBLEStatus(){
  if(!pStatusChar)return;
  String s="{\"r\":"+(String)(sys.relayOn?1:0)+",\"m\":"+(String)(sys.manualOverride?1:0)+
  ",\"e\":"+(String)(sys.emergencyMode?1:0)+",\"t\":"+(String)(sys.timerActive?1:0)+"}";
  pStatusChar->setValue(s.c_str()); pStatusChar->notify();
}
#endif

// ═══════════════════════════════════════════════════════
//  ESP-NOW
// ═══════════════════════════════════════════════════════
#ifdef USE_ESPNOW
void onESPNowRx(const esp_now_recv_info_t*i,const uint8_t*d,int l){
  if(l!=sizeof(ESPNowMsg))return;
  memcpy(&espNowRx,d,sizeof(espNowRx));
  addLog("ESP-NOW cmd="+String(espNowRx.cmd));
  if(espNowRx.cmd==0){sys.manualOverride=true;sys.manualState=false;setRelay(false);}
  else if(espNowRx.cmd==1){sys.manualOverride=true;sys.manualState=true;setRelay(true);}
  else if(espNowRx.cmd==2){sys.manualOverride=true;sys.manualState=!sys.relayOn;setRelay(sys.manualState);}
}
void onESPNowTx(const wifi_tx_info_t *txInfo, esp_now_send_status_t status) {}
void initESPNow(){
  if(esp_now_init()!=ESP_OK){addLog("ESP-NOW: فشل");return;}
  esp_now_register_recv_cb(onESPNowRx); esp_now_register_send_cb(onESPNowTx);
  esp_now_peer_info_t p={};
  memcpy(p.peer_addr,ESPNOW_PEER_MAC,6); p.channel=0; p.encrypt=false;
  if(!esp_now_is_peer_exist(ESPNOW_PEER_MAC))
    if(esp_now_add_peer(&p)==ESP_OK){espNowReady=true; addLog("ESP-NOW: جاهز");}
}
void espNowBroadcast(){
  if(!espNowReady)return;
  espNowTx={sys.relayOn?1u:0u,(uint32_t)millis(),0,0,sys.relayOn};
  #ifdef USE_DHT_SENSOR
  espNowTx.temp=sensorOK?temperature:0; espNowTx.hum=sensorOK?humidity:0;
  #endif
  esp_now_send(ESPNOW_PEER_MAC,(uint8_t*)&espNowTx,sizeof(espNowTx));
}
#endif

// ═══════════════════════════════════════════════════════
//  WebSocket
// ═══════════════════════════════════════════════════════
#ifdef USE_WEBSOCKET
void wsEvent(uint8_t num, WStype_t type, uint8_t* payload, size_t len) {
  switch(type) {
    case WStype_CONNECTED: {
      addLog("WS #" + String(num) + " اتصل");
      String status = buildStatusJson();
      wsServer.sendTXT(num, status);
      break;
    }
    case WStype_DISCONNECTED:
      addLog("WS #" + String(num) + " انفصل");
      break;
    case WStype_TEXT: {
      String cmd = (char*)payload;
      cmd.trim();
      cmd.toLowerCase();
      if (cmd == "on") { sys.manualOverride = true; sys.manualState = true; setRelay(true); }
      else if (cmd == "off") { sys.manualOverride = true; sys.manualState = false; setRelay(false); }
      else if (cmd == "auto") { sys.manualOverride = false; }
      else if (cmd == "toggle") { sys.manualOverride = true; sys.manualState = !sys.relayOn; setRelay(sys.manualState); }
      else if (cmd == "nextpage") { sys.displayPage = (sys.displayPage + 1) % 5; }
      else if (cmd == "emergency") {
        sys.emergencyMode = !sys.emergencyMode;
        if (sys.emergencyMode) { sys.manualOverride = true; sys.manualState = true; setRelay(true); }
        else { sys.manualOverride = false; }
      }
      else if (cmd.startsWith("timer:")) {
        int m = cmd.substring(6).toInt();
        if (m > 0) {
          sys.timerEnd = millis() + (unsigned long)m * 60000;
          sys.timerActive = true;
          sys.manualOverride = true;
          sys.manualState = true;
          setRelay(true);
        }
      }
      String status = buildStatusJson();
      wsServer.broadcastTXT(status);
      break;
    }
    default:
      break;
  }
}
void wsBroadcast() {
  String status = buildStatusJson();
  wsServer.broadcastTXT(status);
}
#endif

// ═══════════════════════════════════════════════════════
//  OLED
// ═══════════════════════════════════════════════════════
void drawProgressBar(int x,int y,int w,int h,int pct){
  display.drawFrame(x,y,w,h);
  int fw=((w-2)*pct)/100;
  if(fw>0)display.drawBox(x+1,y+1,fw,h-2);
}

void animatedSplash(){
  for(int p=0;p<=100;p+=10){
    display.firstPage();
    do{
      display.setFont(u8g2_font_5x7_tr);
      display.setCursor(4,14); display.print("SMART BILLBOARD v6.0");
      display.setCursor(4,26); display.print("ULTIMATE EDITION");
      drawProgressBar(10,36,108,9,p);
      char pc[8]; sprintf(pc,"%d%%",p); display.setCursor(56,55); display.print(pc);
      const char*feats[]={"","BLE+WS","mDNS+NOW","Self-Update","READY!"};
      display.setCursor(4,55); display.print(feats[p/25]);
    }while(display.nextPage());
    delay(80);
  }
  delay(600);
}

void errorScreen(const char*t,const char*d){
  for(int i=0;i<3;i++){
    display.firstPage();
    do{
      if(i%2==1){display.drawBox(0,0,128,64);display.setDrawColor(0);}
      display.setFont(u8g2_font_5x7_tr);
      display.setCursor(4,14);display.print("!! ERROR !!");
      display.setCursor(4,30);display.print(t);
      display.setCursor(4,42);display.print(d);
      display.setDrawColor(1);
    }while(display.nextPage());
    delay(350);
  }
}

String getUptimeStr(){
  unsigned long s=(millis()-sys.bootTime)/1000;
  int d=s/86400;s%=86400;int h=s/3600;s%=3600;int m=s/60;s%=60;
  char b[32];sprintf(b,"%dd %02dh %02dm",d,h,m);return String(b);
}

String getTimeStr(){
  struct tm t;if(!getLocalTime(&t))return "--:--";
  char b[6];sprintf(b,"%02d:%02d",t.tm_hour,t.tm_min);return String(b);
}

String getDayName(int dow){
  const char*n[]={"الأحد","الاثنين","الثلاثاء","الأربعاء","الخميس","الجمعة","السبت"};
  return String(n[dow%7]);
}

void drawPage0(){
  struct tm t;char ts[6]="--:--",ds[12]="--/--/----";
  if(getLocalTime(&t)){sprintf(ts,"%02d:%02d",t.tm_hour,t.tm_min);sprintf(ds,"%02d/%02d/%04d",t.tm_mday,t.tm_mon+1,t.tm_year+1900);}
  display.firstPage();do{
    display.setFont(u8g2_font_logisoso16_tf);
    display.setCursor(0,20);display.print(ts);
    display.setFont(u8g2_font_5x7_tr);
    display.setCursor(68,12);display.print(ds);
    display.drawHLine(0,22,128);
    display.setCursor(0,32);
    if(sys.emergencyMode)display.print("MODE: EMERGENCY");
    else if(sys.timerActive)display.print("MODE: TIMER");
    else if(sys.manualOverride)display.print("MODE: MANUAL");
    else display.print("MODE: AUTO");
    display.setCursor(0,42);display.print("STA: ");display.print(sys.relayOn?"ON ":"OFF");
    if(sys.controlLocked){display.setCursor(80,42);display.print("[LOCK]");}
    display.drawHLine(0,44,128);
    display.setCursor(0,54);
    char sch[28];sprintf(sch,"SCHED %02d:00→%02d:00",sys.startHour,sys.endHour==0?24:sys.endHour);
    display.print(sch);
    display.drawHLine(0,55,128);
    display.setCursor(0,64);
    String ip=sys.wifiConnected?WiFi.localIP().toString():"No WiFi";
    display.print(ip.c_str());
  }while(display.nextPage());
}
void drawPage1(){
  display.firstPage();do{
    display.setFont(u8g2_font_5x7_tr);
    display.setCursor(0,10);display.print("-- Statistics --");
    display.drawHLine(0,12,128);
    char b[28];
    sprintf(b,"Total : %.2fh",stats.totalOnSec/3600.0);display.setCursor(0,24);display.print(b);
    sprintf(b,"Today : %.2fh",stats.todayOnSec/3600.0);display.setCursor(0,34);display.print(b);
    sprintf(b,"Peak  : %.2fh",sys.peakOnHours);display.setCursor(0,44);display.print(b);
    sprintf(b,"kWh   : %.3f",stats.estimatedKWh);display.setCursor(0,54);display.print(b);
    sprintf(b,"Cycles: %lu",(unsigned long)stats.powerCycles);display.setCursor(0,64);display.print(b);
  }while(display.nextPage());
}
void drawPage2(){
  display.firstPage();do{
    display.setFont(u8g2_font_5x7_tr);
    display.setCursor(0,10);display.print("-- Network v6.0 --");
    display.drawHLine(0,12,128);
    char b[28];
    sprintf(b,"IP: %s",sys.wifiConnected?WiFi.localIP().toString().c_str():"---");
    display.setCursor(0,24);display.print(b);
    sprintf(b,"RSSI: %d dBm",WiFi.RSSI());display.setCursor(0,34);display.print(b);
    #ifdef USE_BLE
    sprintf(b,"BLE: %s",bleConnected?"Connected":"Advertising");display.setCursor(0,44);display.print(b);
    #endif
    sprintf(b,"FS: %luKB/%luKB",(unsigned long)FS_IMPL.usedBytes()/1024,(unsigned long)FS_IMPL.totalBytes()/1024);
    display.setCursor(0,54);display.print(b);
    display.setCursor(0,64);display.print(sys.fwVersion.c_str());
    if(sys.updateAvailable){display.setCursor(60,64);display.print("[UPD!]");}
  }while(display.nextPage());
}
void drawPage3(){
  display.firstPage();do{
    display.setFont(u8g2_font_5x7_tr);
    display.setCursor(0,10);display.print("-- Schedule Rules --");
    display.drawHLine(0,12,128);
    #ifdef USE_ADVANCED_SCHEDULE
    int y=24,shown=0;
    for(auto&r:scheduleRules){if(!r.enabled)continue;if(shown>=4)break;
      char b[28];sprintf(b,"%s %02d->%02d",r.name.substring(0,8).c_str(),r.startH,r.endH);
      display.setCursor(0,y);display.print(b);y+=10;shown++;}
      if(shown==0){display.setCursor(0,34);display.print("No active rules");}
      #endif
  }while(display.nextPage());
}
void drawPage4(){
  display.firstPage();do{
    display.setFont(u8g2_font_5x7_tr);
    display.setCursor(0,10);display.print("-- System Health --");
    display.drawHLine(0,12,128);
    char b[28];
    sprintf(b,"Heap: %luKB",(unsigned long)ESP.getFreeHeap()/1024);display.setCursor(0,24);display.print(b);
    sprintf(b,"Uptime: %s",getUptimeStr().c_str());display.setCursor(0,34);display.print(b);
    sprintf(b,"Msgs: %lu",(unsigned long)sys.msgCount);display.setCursor(0,44);display.print(b);
    sprintf(b,"v%s",sys.fwVersion.c_str());display.setCursor(0,54);display.print(b);
    if(sys.updateAvailable){display.setCursor(0,64);display.print("UPDATE AVAILABLE!");}
    else{sprintf(b,"Users: %d",(int)users.size());display.setCursor(0,64);display.print(b);}
  }while(display.nextPage());
}

void updateDisplay(){
  switch(sys.displayPage%5){
    case 0:drawPage0();break;case 1:drawPage1();break;
    case 2:drawPage2();break;case 3:drawPage3();break;case 4:drawPage4();break;
  }
  struct tm t;if(!getLocalTime(&t))return;
  int h=t.tm_hour;
  bool night=(nightDimStart<nightDimEnd)?(h>=nightDimStart&&h<nightDimEnd):(h>=nightDimStart||h<nightDimEnd);
  int target=night?nightContrast:dayContrast;
  if(target!=sys.oledContrast){display.setContrast(target);sys.oledContrast=target;}
}

// ═══════════════════════════════════════════════════════
//  منطق المرحل
// ═══════════════════════════════════════════════════════
bool shouldBeOn(){
  if(sys.emergencyMode)return true;
  if(sys.manualOverride){
    if(sys.timerActive&&millis()>=sys.timerEnd){
      sys.timerActive=false;sys.manualOverride=false;addLog("انتهى المؤقت");
    }else return sys.manualState;
  }
  #ifdef USE_ADVANCED_SCHEDULE
  if(sys.useAdvancedSchedule)return evalAdvancedSchedule();
  #endif
  struct tm t;if(!getLocalTime(&t))return false;
  int h=t.tm_hour;
  if(sys.endHour==0)return h>=sys.startHour;
  if(sys.startHour<sys.endHour)return h>=sys.startHour&&h<sys.endHour;
  return h>=sys.startHour||h<sys.endHour;
}

void setRelay(bool on){
  bool was=sys.relayOn;sys.relayOn=on;
  digitalWrite(Config::RELAY_PIN,on?LOW:HIGH);
  digitalWrite(Config::STATUS_LED,on?HIGH:LOW);
  if(!was&&on){stats.lastOnTime=millis();stats.powerCycles++;stats.dailyCycles++;}
  if(was&&!on&&stats.lastOnTime>0){
    unsigned long s=(millis()-stats.lastOnTime)/1000;
    stats.totalOnSec+=s;stats.todayOnSec+=s;
    stats.estimatedKWh+=(LOAD_WATTS*s)/3600000.0;
    if(stats.todayOnSec/3600.0>sys.peakOnHours)sys.peakOnHours=stats.todayOnSec/3600.0;
  }
}

void updateRelay(){
  bool desired=shouldBeOn();
  if(desired!=sys.relayOn){
    setRelay(desired);
    addLog(String("مرحل: ")+(desired?"ON":"OFF")+" [auto]");
    if(mqtt.connected())mqtt.publish(mqttTopicState.c_str(),buildStatusJson().c_str(),true);
    #ifdef USE_ESPNOW
    espNowBroadcast();
    #endif
    #ifdef USE_WEBSOCKET
    wsBroadcast();
    #endif
    #ifdef USE_BLE
    updateBLEStatus();
    #endif
  }
}

// ═══════════════════════════════════════════════════════
//  الزر الذكي
// ═══════════════════════════════════════════════════════
namespace SmartButton {
  bool lastRaw=HIGH,currentState=HIGH;
  uint8_t clickCount=0;
  uint32_t lastClickTime=0,pressStart=0;
  bool isPressed=false,longHandled=false;
  constexpr uint16_t DEBOUNCE=40,CLICK_TO=400,LONG_MS=1500,VLONG_MS=8000;

  void blink(int n){for(int i=0;i<n;i++){digitalWrite(Config::STATUS_LED,HIGH);delay(60);digitalWrite(Config::STATUS_LED,LOW);delay(60);}}
  void dispatch(uint8_t c,bool lp,bool vl);

  void update(){
    bool raw=digitalRead(Config::BUTTON_PIN);
    static uint32_t lc=0;
    if(raw!=lastRaw){lc=millis();lastRaw=raw;}
    if(millis()-lc<DEBOUNCE)return;
    bool pressed=(raw==LOW);
    if(pressed&&!isPressed){isPressed=true;pressStart=millis();longHandled=false;}
    if(isPressed&&pressed){
      uint32_t held=millis()-pressStart;
      if(!longHandled&&held>=VLONG_MS){longHandled=true;dispatch(0,false,true);}
      else if(!longHandled&&held>=LONG_MS){
        static uint32_t lb=0;
        if(millis()-lb>500){digitalWrite(Config::STATUS_LED,!digitalRead(Config::STATUS_LED));lb=millis();}
      }
    }
    if(!pressed&&isPressed){
      isPressed=false;
      uint32_t held=millis()-pressStart;
      if(!longHandled){
        if(held>=LONG_MS)dispatch(0,true,false);
        else{clickCount++;lastClickTime=millis();}
      }
      longHandled=false;
    }
    if(clickCount>0&&!isPressed&&millis()-lastClickTime>CLICK_TO){
      uint8_t c=clickCount;clickCount=0;dispatch(c,false,false);
    }
  }
}

void SmartButton::dispatch(uint8_t clicks,bool lp,bool vl){
  if(sys.captivePortalActive){if(lp||vl)ESP.restart();return;}
  if(vl){blink(5);addLog("زر: Captive Portal");
    #ifdef USE_CAPTIVE_PORTAL
    // startCaptivePortal() will be defined later
    #endif
    return;}
    if(lp){sys.controlLocked=!sys.controlLocked;blink(sys.controlLocked?3:1);
      audit("button","lock:"+(String)(sys.controlLocked?"on":"off"));addLog("زر: "+(String)(sys.controlLocked?"قفل":"فتح"));updateDisplay();return;}
      if(sys.controlLocked){blink(1);return;}
      if(clicks>=3){
        sys.emergencyMode=!sys.emergencyMode;blink(3);
        if(sys.emergencyMode){sys.manualOverride=true;sys.manualState=true;setRelay(true);addLog("زر: طارئ ON");
          bot.sendMessage(Config::ALLOWED_CHATS[0],"🚨 *وضع الطارئ!* تشغيل قسري\n/auto للإلغاء","Markdown");}
          else{sys.manualOverride=false;updateRelay();addLog("زر: إلغاء الطارئ");
            bot.sendMessage(Config::ALLOWED_CHATS[0],"✅ تم إلغاء وضع الطارئ");}
            audit("button","emergency:"+(String)sys.emergencyMode);updateDisplay();return;}
            if(clicks==2){sys.displayPage++;blink(2);addLog("زر: صفحة "+String(sys.displayPage%5));updateDisplay();return;}
            if(clicks==1){
              if(sys.emergencyMode)return;
              sys.manualOverride=true;sys.manualState=!sys.relayOn;sys.timerActive=false;
              setRelay(sys.manualState);blink(1);
              audit("button","toggle:"+(String)sys.manualState);addLog("زر: "+(String)(sys.manualState?"ON":"OFF"));
              updateDisplay();
              #ifdef USE_WEBSOCKET
              wsBroadcast();
              #endif
              #ifdef USE_BLE
              updateBLEStatus();
              #endif
            }
}

// ═══════════════════════════════════════════════════════
//  MQTT
// ═══════════════════════════════════════════════════════
void mqttCallback(char*topic,byte*payload,unsigned int len){
  String cmd;for(unsigned int i=0;i<len;i++)cmd+=(char)payload[i];
  cmd.trim();cmd.toLowerCase();addLog("MQTT: "+cmd);
  if(cmd=="on"){sys.manualOverride=true;sys.manualState=true;setRelay(true);}
  else if(cmd=="off"){sys.manualOverride=true;sys.manualState=false;setRelay(false);}
  else if(cmd=="auto"){sys.manualOverride=false;updateRelay();}
  else if(cmd=="toggle"){sys.manualOverride=true;sys.manualState=!sys.relayOn;setRelay(sys.manualState);}
  else if(cmd=="status")mqtt.publish(mqttTopicState.c_str(),buildStatusJson().c_str(),true);
  audit("mqtt","cmd:"+cmd,"MQTT");
}
void mqttReconnect(){
  if(mqttServer.length()==0||mqtt.connected())return;
  String cid=mqttClientId+String(random(0xffff),HEX);
  bool ok=(mqttUser.length()>0)?mqtt.connect(cid.c_str(),mqttUser.c_str(),mqttPassStr.c_str()):mqtt.connect(cid.c_str());
  if(ok){mqtt.subscribe(mqttTopicCmd.c_str());addLog("MQTT متصل");}
}

// ═══════════════════════════════════════════════════════
//  Captive Portal
// ═══════════════════════════════════════════════════════
#ifdef USE_CAPTIVE_PORTAL
bool cpSaved=false;String cpSSID="",cpPass="";

void startCaptivePortal(){
  WiFi.disconnect(true);delay(100);
  WiFi.softAP(Config::CAPTIVE_SSID,strlen(Config::CAPTIVE_PASS)>0?Config::CAPTIVE_PASS:nullptr);
  dnsServer.start(53,"*",WiFi.softAPIP());
  sys.captivePortalActive=true;
  addLog("Captive Portal: "+String(Config::CAPTIVE_SSID));
  webServer.on("/",[](){ webServer.send(200,"text/html",
    R"(<html><head><meta charset=UTF-8><title>WiFi Setup</title></head><body dir=rtl style='font-family:Arial;text-align:center;padding:40px;background:#1a1a2e;color:#eee'>)"
    R"(<h2>📡 Smart Billboard v6.0</h2><form action='/save' method='POST'>)"
    R"(<p>SSID: <input name=ssid></p><p>Password: <input name=pass type=password></p>)"
    R"(<input type=submit value='حفظ والاتصال'></form></body></html>)");});
  webServer.on("/save",HTTP_POST,[](){
    cpSSID=webServer.arg("ssid");cpPass=webServer.arg("pass");cpSaved=true;
    webServer.send(200,"text/plain","OK - Rebooting...");});
  webServer.onNotFound([](){webServer.sendHeader("Location","http://192.168.4.1/");webServer.send(302);});
  webServer.begin();
}
void handleCaptivePortal(){
  if(!sys.captivePortalActive)return;
  dnsServer.processNextRequest();webServer.handleClient();
  if(cpSaved){cpSaved=false;
    #ifdef USE_LITTLEFS
    DynamicJsonDocument d(256);d["ssid"]=cpSSID;d["pass"]=cpPass;
    File f=FS_IMPL.open("/wifi_extra.json","w");serializeJson(d,f);f.close();
    #endif
    delay(2000);dnsServer.stop();WiFi.softAPdisconnect(true);
    sys.captivePortalActive=false;ESP.restart();}
}
#endif

// ═══════════════════════════════════════════════════════
//  دوال مساعدة
// ═══════════════════════════════════════════════════════
bool isAllowedChat(const String&id){for(int i=0;i<Config::ALLOWED_COUNT;i++)if(Config::ALLOWED_CHATS[i]==id)return true;return false;}
bool isChatAuth(const String&id){if(adminPassword.length()==0)return true;for(auto&c:authenticatedChats)if(c==id)return true;return false;}
void checkDailyReset(){
  struct tm t;if(!getLocalTime(&t))return;
  if(t.tm_mday!=stats.lastLogDay){
    recordDailyStats();
    stats.todayOnSec=0;stats.dailyCycles=0;stats.lastLogDay=t.tm_mday;
    addLog("إعادة ضبط يومية");
  }
}

// ================================
//  استدعاء وحفظ Dashboard HTML
// ================================
bool fetchDashboardHTML() {
  if (!sys.wifiConnected) return false;

  HTTPClient http;
  http.begin(Config::DASHBOARD_URL);
  http.addHeader("User-Agent", "SmartBillboard/6.0");
  int code = http.GET();
  if (code == 200) {
    String html = http.getString();
    http.end();
    if (html.length() > 100) {   // sanity check
      File f = FS_IMPL.open("/dashboard.html", "w");
      if (f) {
        f.print(html);
        f.close();
        addLog("Dashboard HTML downloaded (" + String(html.length()) + " bytes)");
        audit("system", "dashboard_update", WiFi.localIP().toString());
        return true;
      } else {
        addLog("ERROR writing dashboard.html");
      }
    } else {
      addLog("Dashboard HTML too short, ignored");
    }
  } else {
    http.end();
    addLog("GitHub fetch failed, HTTP " + String(code));
  }
  return false;
}

// ================================
//  خدمة الملف (مع احتياطي)
// ================================
void serveDashboard() {
  if (FS_IMPL.exists("/dashboard.html")) {
    File f = FS_IMPL.open("/dashboard.html", "r");
    if (f) {
      webServer.streamFile(f, "text/html; charset=utf-8");
      f.close();
      return;
    }
  }
  // fallback للصفحة المضمنة
  webServer.send(200, "text/html; charset=utf-8", buildDashboardHTML());
}

// ═══════════════════════════════════════════════════════
//  Dashboard HTML (مختصرة للإرسال)
// ═══════════════════════════════════════════════════════
String buildDashboardHTML() {
  return R"HTML(<!DOCTYPE html><html lang="ar" dir="rtl"><head><meta charset="UTF-8"><title>Smart Billboard v6.0</title></head><body style="background:#0a0a1a;color:#fff;font-family:sans-serif;text-align:center;padding:40px"><h1>🚀 Smart Billboard v6.0 ULTIMATE</h1><p>Works with API & WebSocket</p><script>var ws=new WebSocket('ws://'+location.hostname+':81/');ws.onmessage=function(e){console.log(e.data);};</script></body></html>)HTML";
}

// ═══════════════════════════════════════════════════════
//  تسجيل مسارات الـ API
// ═══════════════════════════════════════════════════════
void registerAPIRoutes() {
  webServer.on("/", HTTP_GET, serveDashboard);
  webServer.on("/status", HTTP_GET, [](){ webServer.send(200,"application/json", buildStatusJson()); });
  webServer.on("/log.json", HTTP_GET, [](){
    String json="["; int cnt=min(logIdx,LOG_SIZE);
    for(int j=cnt-1;j>=0;j--){
      int idx=(logIdx-1-j+LOG_SIZE)%LOG_SIZE;
      json+="{\"ts\":"+String(eventLog[idx%LOG_SIZE].ts)+",\"msg\":\""+eventLog[idx%LOG_SIZE].msg+"\"}";
      if(j>0)json+=",";
    }
    webServer.send(200,"application/json",json+"]");
  });
  webServer.on("/cmd", HTTP_GET, [](){
    String ip=webServer.client().remoteIP().toString();
    if(!checkRateLimit(ip)){webServer.send(429,"application/json","{\"error\":\"rate limit\"}");return;}
    String a=webServer.arg("action");
    if(a=="on"){sys.manualOverride=true;sys.manualState=true;setRelay(true);}
    else if(a=="off"){sys.manualOverride=true;sys.manualState=false;setRelay(false);}
    else if(a=="auto"){sys.manualOverride=false;updateRelay();}
    else if(a=="toggle"){sys.manualOverride=true;sys.manualState=!sys.relayOn;setRelay(sys.manualState);}
    else if(a=="reboot"){webServer.send(200,"application/json","{\"ok\":true}");delay(1000);saveConfigJSON();ESP.restart();}
    else if(a=="resetstats"){stats.totalOnSec=0;stats.todayOnSec=0;stats.powerCycles=0;stats.estimatedKWh=0;saveConfigJSON();}
    else if(a=="saveconfig"){saveConfigJSON();saveScheduleJSON();saveUsersJSON();}
    else if(a=="setsched"){sys.startHour=webServer.arg("start").toInt();sys.endHour=webServer.arg("end").toInt();saveConfigJSON();updateRelay();}
    else if(a=="advsched:on"){sys.useAdvancedSchedule=true;saveConfigJSON();}
    else if(a=="advsched:off"){sys.useAdvancedSchedule=false;saveConfigJSON();}
    audit("api","cmd:"+a,ip);
    webServer.sendHeader("Location","/");webServer.send(302);
  });

  #ifdef USE_LITTLEFS
  webServer.on("/api/fs", HTTP_GET, [](){ webServer.send(200,"application/json",getFSInfo()); });
  webServer.on("/api/file", HTTP_GET, [](){
    String path=webServer.arg("path");
    if(path.length()==0||!FS_IMPL.exists(path)){webServer.send(404,"text/plain","Not Found");return;}
    File f=FS_IMPL.open(path,"r");
    webServer.streamFile(f,"application/octet-stream");
    f.close();
  });
  webServer.on("/api/file", HTTP_POST, [](){
    String body=webServer.arg("plain");
    DynamicJsonDocument doc(8192);
    if(deserializeJson(doc,body)){webServer.send(400,"application/json","{\"ok\":false}");return;}
    String path=doc["path"].as<String>();
    String content=doc["content"].as<String>();
    if(writeFile(path,content)){
      if(path=="/config.json")loadConfigJSON();
      else if(path=="/schedule.json")loadScheduleJSON();
      else if(path=="/users.json")loadUsersJSON();
      audit("api","file_write:"+path,webServer.client().remoteIP().toString());
      webServer.send(200,"application/json","{\"ok\":true}");
    } else webServer.send(500,"application/json","{\"ok\":false}");
  });
  webServer.on("/api/file", HTTP_DELETE, [](){
    String path=webServer.arg("path");
    if(FS_IMPL.remove(path))webServer.send(200,"application/json","{\"ok\":true}");
    else webServer.send(500,"application/json","{\"ok\":false}");
  });
  webServer.on("/api/upload", HTTP_POST, [](){ webServer.send(200,"application/json","{\"ok\":true}"); },[](){
    HTTPUpload& upload=webServer.upload();
    static File uploadFile;
    if(upload.status==UPLOAD_FILE_START){
      String path="/"+upload.filename;
      uploadFile=FS_IMPL.open(path,"w");
    } else if(upload.status==UPLOAD_FILE_WRITE){
      if(uploadFile)uploadFile.write(upload.buf,upload.currentSize);
    } else if(upload.status==UPLOAD_FILE_END){
      if(uploadFile){uploadFile.close();audit("api","upload:"+upload.filename);}
    }
  });
  webServer.on("/api/backup", HTTP_GET, [](){
    webServer.sendHeader("Content-Disposition","attachment; filename=billboard_backup.json");
    webServer.send(200,"application/json",buildFullBackup());
  });
  webServer.on("/api/restore", HTTP_POST, [](){
    String body=webServer.arg("plain");
    if(restoreFromBackup(body)){
      loadConfigJSON();loadScheduleJSON();loadUsersJSON();
      audit("api","restore",webServer.client().remoteIP().toString());
      webServer.send(200,"application/json","{\"ok\":true}");
    } else webServer.send(400,"application/json","{\"ok\":false}");
  });
  #endif

  webServer.on("/api/ota", HTTP_POST, [](){ webServer.send(200,"application/json","{\"ok\":true}"); },[](){
    HTTPUpload& u=webServer.upload();
    if(u.status==UPLOAD_FILE_START){
      addLog("OTA API: "+String(u.filename));
      if(!Update.begin(UPDATE_SIZE_UNKNOWN)){addLog("OTA: فشل البدء");return;}
    } else if(u.status==UPLOAD_FILE_WRITE){
      if(Update.write(u.buf,u.currentSize)!=u.currentSize){addLog("OTA: خطأ كتابة");}
    } else if(u.status==UPLOAD_FILE_END){
      if(Update.end(true)){addLog("OTA: اكتمل → إعادة تشغيل");delay(1000);ESP.restart();}
      else addLog("OTA: فشل الإنهاء");
    }
  });

  #ifdef USE_ADVANCED_SCHEDULE
  webServer.on("/api/rules", HTTP_GET, [](){
    DynamicJsonDocument doc(8192);
    JsonArray arr=doc.createNestedArray("rules");
    for(auto&r:scheduleRules){
      JsonObject o=arr.createNestedObject();
      o["name"]=r.name;o["type"]=r.type;o["priority"]=r.priority;
      o["enabled"]=r.enabled;o["forceOn"]=r.forceOn;
      o["startH"]=r.startH;o["startM"]=r.startM;
      o["endH"]=r.endH;o["endM"]=r.endM;
      o["weekDays"]=r.weekDays;o["triggers"]=r.triggerCount;
      o["expiry"]=(long long)r.expiryDate;
    }
    String out;serializeJson(doc,out);webServer.send(200,"application/json",out);
  });
  webServer.on("/api/rules", HTTP_POST, [](){
    String body=webServer.arg("plain");
    DynamicJsonDocument doc(2048);
    if(deserializeJson(doc,body)){webServer.send(400,"application/json","{\"ok\":false}");return;}
    ScheduleRule r;
    r.name=doc["name"].as<String>();r.type=doc["type"]|0;r.priority=doc["priority"]|1;
    r.enabled=doc["enabled"]|true;r.forceOn=doc["forceOn"]|true;
    r.startH=doc["startH"]|19;r.startM=doc["startM"]|0;
    r.endH=doc["endH"]|0;r.endM=doc["endM"]|0;
    r.weekDays=doc["weekDays"]|0x7F;r.month=0;r.day=0;r.seasonStart=0;r.seasonEnd=0;
    r.expiryDate=(time_t)(long long)doc["expiry"]|0;r.triggerCount=0;
    scheduleRules.push_back(r);saveScheduleJSON();
    audit("api","rule_add:"+r.name,webServer.client().remoteIP().toString());
    webServer.send(200,"application/json","{\"ok\":true}");
  });
  webServer.onNotFound([](){
    String url=webServer.uri();
    if(url.startsWith("/api/rules/")){
      int idx=url.substring(11).toInt();
      if(idx<0||idx>=(int)scheduleRules.size()){webServer.send(404,"application/json","{\"error\":\"not found\"}");return;}
      if(webServer.method()==HTTP_DELETE){scheduleRules.erase(scheduleRules.begin()+idx);saveScheduleJSON();webServer.send(200,"application/json","{\"ok\":true}");}
      else if(webServer.method()==HTTP_PATCH){
        DynamicJsonDocument doc(512);deserializeJson(doc,webServer.arg("plain"));
        if(doc.containsKey("enabled"))scheduleRules[idx].enabled=doc["enabled"];
        saveScheduleJSON();webServer.send(200,"application/json","{\"ok\":true}");
      }
    } else webServer.send(404,"text/plain","Not Found");
  });
    #endif

    webServer.on("/api/users", HTTP_GET, [](){
      DynamicJsonDocument doc(4096);
      JsonArray arr=doc.createNestedArray("users");
      for(auto&u:users){ JsonObject o=arr.createNestedObject(); o["username"]=u.username;o["role"]=(int)u.role;o["enabled"]=u.enabled; }
      String out;serializeJson(doc,out);webServer.send(200,"application/json",out);
    });
    webServer.on("/api/users", HTTP_POST, [](){
      DynamicJsonDocument doc(1024);
      if(deserializeJson(doc,webServer.arg("plain"))){webServer.send(400,"application/json","{\"ok\":false}");return;}
      String uname=doc["username"].as<String>(); String pass=doc["password"].as<String>(); int role=doc["role"]|0;
      users.push_back({uname,md5Hash(pass+"billboard"),(UserRole)role,true});
      saveUsersJSON();
      audit("api","user_add:"+uname,webServer.client().remoteIP().toString());
      webServer.send(200,"application/json","{\"ok\":true}");
    });
    webServer.on("/api/users/password", HTTP_POST, [](){
      DynamicJsonDocument doc(512);
      if(deserializeJson(doc,webServer.arg("plain"))){webServer.send(400,"application/json","{\"ok\":false}");return;}
      String uname=doc["username"].as<String>(); String pass=doc["password"].as<String>();
      bool found=false;
      for(auto&u:users){if(u.username==uname){u.passHash=md5Hash(pass+"billboard");found=true;break;}}
      if(found){saveUsersJSON();audit("api","passwd:"+uname,webServer.client().remoteIP().toString());webServer.send(200,"application/json","{\"ok\":true}");}
      else webServer.send(404,"application/json","{\"ok\":false}");
    });

    webServer.on("/api/login", HTTP_POST, [](){
      DynamicJsonDocument doc(512);
      if(deserializeJson(doc,webServer.arg("plain"))){webServer.send(400,"application/json","{\"error\":\"invalid json\"}");return;}
      String uname=doc["username"].as<String>(); String pass=doc["password"].as<String>();
      String ip=webServer.client().remoteIP().toString();
      String hash=md5Hash(pass+"billboard");
      for(auto&u:users){
        if(u.username==uname&&u.passHash==hash&&u.enabled){
          String tk=createAuthToken(uname,u.role,ip);
          audit("api","login:"+uname,ip);
          webServer.send(200,"application/json","{\"token\":\""+tk+"\",\"role\":"+(String)(int)u.role+"}");
          return;
        }
      }
      audit("api","login_fail:"+uname,ip,false);
      webServer.send(401,"application/json","{\"error\":\"invalid credentials\"}");
    });
    webServer.on("/api/revoke", HTTP_POST, [](){ tokens.clear(); audit("api","revoke_all",webServer.client().remoteIP().toString()); webServer.send(200,"application/json","{\"ok\":true}"); });
    webServer.on("/api/audit", HTTP_GET, [](){
      DynamicJsonDocument doc(8192); JsonArray arr=doc.createNestedArray("entries");
      int cnt=min(auditIdx,AUDIT_SIZE);
      for(int i=cnt-1;i>=0;i--){
        int idx=(auditIdx-1-i+AUDIT_SIZE)%AUDIT_SIZE;
        JsonObject o=arr.createNestedObject();
        o["ts"]=auditLog[idx%AUDIT_SIZE].ts;o["user"]=auditLog[idx%AUDIT_SIZE].user;
        o["action"]=auditLog[idx%AUDIT_SIZE].action;o["ip"]=auditLog[idx%AUDIT_SIZE].ip;
        o["ok"]=auditLog[idx%AUDIT_SIZE].success;
      }
      String out;serializeJson(doc,out);webServer.send(200,"application/json",out);
    });
    webServer.on("/api/whitelist", HTTP_POST, [](){
      DynamicJsonDocument doc(2048);
      if(deserializeJson(doc,webServer.arg("plain"))){webServer.send(400,"application/json","{\"ok\":false}");return;}
      ipWhitelist.clear();
      for(String ip:doc["ips"].as<JsonArray>())if(ip.length()>0)ipWhitelist.push_back(ip);
      saveUsersJSON();webServer.send(200,"application/json","{\"ok\":true}");
    });
    webServer.on("/api/mqtt", HTTP_POST, [](){
      DynamicJsonDocument doc(1024);
      if(deserializeJson(doc,webServer.arg("plain"))){webServer.send(400,"application/json","{\"ok\":false}");return;}
      mqttServer=doc["server"].as<String>();mqttPort=doc["port"]|1883;
      mqttUser=doc["user"].as<String>();mqttPassStr=doc["pass"].as<String>();
      mqtt.disconnect();mqtt.setServer(mqttServer.c_str(),mqttPort);
      saveConfigJSON();webServer.send(200,"application/json","{\"ok\":true}");
    });

    ElegantOTA.begin(&webServer,Config::OTA_USER,Config::OTA_PASS);
    ElegantOTA.onStart([](){sys.otaInProgress=true;addLog("OTA: بدأ");});
    ElegantOTA.onEnd([](bool ok){sys.otaInProgress=!ok;addLog(ok?"OTA: ✓":"OTA: ✗"); bot.sendMessage(Config::ALLOWED_CHATS[0],ok?"✅ تحديث ناجح":"❌ تحديث فشل");});
    webServer.begin();
    addLog("Web API: :"+String(Config::WEB_PORT));
}

// ═══════════════════════════════════════════════════════
//  معالجة تيليغرام
// ═══════════════════════════════════════════════════════
void handleMessages(int count){
  for(int i=0;i<count;i++){
    String chatId=bot.messages[i].chat_id;
    String text=bot.messages[i].text;
    String fname=bot.messages[i].from_name;
    text.trim();sys.msgCount++;
    if(!isAllowedChat(chatId)){bot.sendMessage(chatId,"⛔ غير مصرح");continue;}
    if(adminPassword.length()>0&&!isChatAuth(chatId)){
      if(text.startsWith("/login ")){
        String pass = text.substring(7);
        pass.trim();
        if(pass==adminPassword){
          authenticatedChats.push_back(chatId);
          audit("telegram","login:"+chatId,fname);
          bot.sendMessage(chatId,"✅ مرحباً "+fname+"!");
        } else { audit("telegram","login_fail:"+chatId,fname,false); bot.sendMessage(chatId,"❌ خاطئة"); }
      } else bot.sendMessage(chatId,"🔐 `/login كلمة_المرور`","Markdown");
      continue;
    }
    addLog("TG: "+text.substring(0,min(30,(int)text.length())));
    audit("telegram",text.substring(0,30),fname);
    if(text=="/on"){sys.manualOverride=true;sys.manualState=true;setRelay(true);bot.sendMessage(chatId,"🟢 تشغيل");}
    else if(text=="/off"){sys.manualOverride=true;sys.manualState=false;setRelay(false);bot.sendMessage(chatId,"🔴 إطفاء");}
    else if(text=="/auto"){sys.manualOverride=false;sys.emergencyMode=false;updateRelay();bot.sendMessage(chatId,"🤖 تلقائي");}
    else if(text=="/toggle"){sys.manualOverride=true;sys.manualState=!sys.relayOn;setRelay(sys.manualState);bot.sendMessage(chatId,sys.relayOn?"🟢":"🔴");}
    else if(text=="/emergency"){sys.emergencyMode=true;sys.manualOverride=true;sys.manualState=true;setRelay(true);bot.sendMessage(chatId,"🚨 طارئ مفعّل\n/auto للإلغاء");}
    else if(text.startsWith("/timer ")){
      int m=text.substring(7).toInt();
      if(m>0&&m<=1440){sys.timerEnd=millis()+(unsigned long)m*60000;sys.timerActive=true;sys.manualOverride=true;sys.manualState=true;setRelay(true);bot.sendMessage(chatId,"⏱️ "+String(m)+" دقيقة");}
    }
    else if(text=="/stoptimer"){sys.timerActive=false;sys.manualOverride=false;updateRelay();bot.sendMessage(chatId,"⏹️ ألغي");}
    else if(text=="/lock"){sys.controlLocked=true;bot.sendMessage(chatId,"🔒 مقفول");updateDisplay();}
    else if(text=="/unlock"){sys.controlLocked=false;bot.sendMessage(chatId,"🔓 مفتوح");updateDisplay();}
    else if(text.startsWith("/setstart ")){int h=text.substring(10).toInt();if(h>=0&&h<24){sys.startHour=h;saveConfigJSON();updateRelay();bot.sendMessage(chatId,"✅ بدء: "+String(h)+":00");}}
    else if(text.startsWith("/setend ")){int h=text.substring(8).toInt();if(h>=0&&h<=24){sys.endHour=h==24?0:h;saveConfigJSON();updateRelay();bot.sendMessage(chatId,"✅ نهاية: "+String(h)+":00");}}
    else if(text.startsWith("/timezone ")){int z=text.substring(10).toInt();if(z>=-12&&z<=14){sys.tzOffset=z*3600;configTime(sys.tzOffset,0,"pool.ntp.org");saveConfigJSON();bot.sendMessage(chatId,"✅ UTC+"+String(z));}}
    else if(text=="/stats"){
      String m="📊 *v6.0 الإحصائيات*\n━━━━━━━━━━━\n";
      m+="⏳ إجمالي: "+String(stats.totalOnSec/3600.0,2)+"h\n";
      m+="📅 اليوم: "+String(stats.todayOnSec/3600.0,2)+"h\n";
      m+="🔝 ذروة: "+String(sys.peakOnHours,2)+"h\n";
      m+="⚡ طاقة: "+String(stats.estimatedKWh,3)+" kWh\n";
      m+="🔄 دورات: "+String(stats.powerCycles)+"\n";
      #ifdef USE_LITTLEFS
      m+="💾 FS: "+String(FS_IMPL.usedBytes()/1024)+"KB/"+String(FS_IMPL.totalBytes()/1024)+"KB\n";
      #endif
      bot.sendMessage(chatId,m,"Markdown");
    }
    else if(text=="/backup"){
      bot.sendMessage(chatId,"💾 إنشاء نسخة احتياطية...");
      String bk=buildFullBackup();
      bot.sendMessage(chatId,"```\n"+bk.substring(0,min(3000,(int)bk.length()))+"\n```\n[عبر الويب للملف الكامل]","Markdown");
    }
    #ifdef USE_SELF_UPDATE
    else if(text=="/checkupdate"){checkGitHubUpdate();}
    else if(text=="/selfupdate"){
      if(sys.updateAvailable)bot.sendMessage(chatId,"⬆ تحديث يجري... تحقق من /ip للمتابعة");
      else bot.sendMessage(chatId,"✅ الجهاز محدّث ("+sys.fwVersion+")");
    }
    #endif
    else if(text=="/network"){
      String m="📡 *الشبكة v6.0*\n━━━━━━━━━━━\n";
      m+="WiFi: `"+WiFi.SSID()+"`\nIP: `"+WiFi.localIP().toString()+"`\nRSSI: "+String(WiFi.RSSI())+" dBm\n";
      #ifdef USE_MDNS
      m+="mDNS: `http://"+String(Config::MDNS_NAME)+".local`\n";
      #endif
      #ifdef USE_BLE
      m += "BLE: " + String(bleConnected ? "✅ متصل" : "📢 يعلن") + "\n";
      #endif
      #ifdef USE_WEBSOCKET
      m+="WS: `ws://"+WiFi.localIP().toString()+":81`\n";
      #endif
      m+="Dashboard: `http://"+WiFi.localIP().toString()+":"+String(Config::WEB_PORT)+"/`\n";
      #ifdef USE_LITTLEFS
      m+="FS: "+String(FS_IMPL.usedBytes()/1024)+"KB/"+String(FS_IMPL.totalBytes()/1024)+"KB";
      #endif
      bot.sendMessage(chatId,m,"Markdown");
    }
    else if(text=="/status"){
      String m="📊 *حالة v6.0 ULTIMATE*\n━━━━━━━━━━━\n";
      m+=String(sys.relayOn?"🟢 تشغيل":"🔴 إطفاء")+"\n";
      m+="وضع: "+String(sys.emergencyMode?"🚨 طارئ":sys.timerActive?"⏱️ مؤقت":sys.manualOverride?"✋ يدوي":"🤖 تلقائي")+"\n";
      m+="قفل: "+String(sys.controlLocked?"🔒":"🔓")+"\n━━━━━━━━━━━\n";
      m+="IP: `"+WiFi.localIP().toString()+"`\nتشغيل: "+getUptimeStr()+"\nRAM: "+String(ESP.getFreeHeap()/1024)+" KB\n";
      m+="━━━━━━━━━━━\n";
      m+="🔧 `http://"+WiFi.localIP().toString()+":"+String(Config::WEB_PORT)+"/`\n";
      m+="Firmware: "+sys.fwVersion+String(sys.updateAvailable?" 🔄 تحديث: "+sys.updateVersion:"");
      bot.sendMessage(chatId,m,"Markdown");
    }
    else if(text=="/log"){
      String m="📋 *السجل*\n━━━━━━━━━━━\n";
      int cnt=min(logIdx,LOG_SIZE);
      for(int j=cnt-1;j>=max(0,cnt-12);j--){
        int idx=(logIdx-1-j+LOG_SIZE)%LOG_SIZE;
        m+="`+"+String(eventLog[idx%LOG_SIZE].ts/1000)+"s` "+eventLog[idx%LOG_SIZE].msg+"\n";
      }
      bot.sendMessage(chatId,m,"Markdown");
    }
    else if(text=="/ip"||text=="/ota"){
      String m="🔧 Dashboard v6.0:\n";
      m+="`http://"+WiFi.localIP().toString()+":"+String(Config::WEB_PORT)+"/`\n";
      #ifdef USE_MDNS
      m+="🌐 `http://"+String(Config::MDNS_NAME)+".local/`\n";
      #endif
      m+="OTA: `http://"+WiFi.localIP().toString()+":"+String(Config::WEB_PORT)+"/update`\n";
      m+="📡 WS: `ws://"+WiFi.localIP().toString()+":81`";
      bot.sendMessage(chatId,m,"Markdown");
    }
    else if(text=="/reboot"){bot.sendMessage(chatId,"🔄 إعادة تشغيل...");delay(1000);saveConfigJSON();saveScheduleJSON();ESP.restart();}
    else if(text=="/resetstats"){stats.totalOnSec=0;stats.todayOnSec=0;stats.powerCycles=0;stats.estimatedKWh=0;sys.peakOnHours=0;saveConfigJSON();bot.sendMessage(chatId,"✅ مصفّاة");}
    else if(text.startsWith("/setpassword ")){
      adminPassword = text.substring(13);
      adminPassword.trim();
      saveConfigJSON();
      bot.sendMessage(chatId, adminPassword.length() == 0 ? "✅ محذوفة" : "✅ محدّثة");
    }
    else if(text=="/logout"){authenticatedChats.erase(std::remove(authenticatedChats.begin(),authenticatedChats.end(),chatId),authenticatedChats.end());bot.sendMessage(chatId,"👋");}
    else if(text=="/nextpage"){sys.displayPage++;updateDisplay();bot.sendMessage(chatId,"🖥️ صفحة "+String(sys.displayPage%5));}
    else if(text.startsWith("/page ")){sys.displayPage=text.substring(6).toInt();updateDisplay();}
    else if(text.startsWith("/setwatts ")){float w=text.substring(10).toFloat();if(w>0&&w<=10000){LOAD_WATTS=w;saveConfigJSON();bot.sendMessage(chatId,"✅ "+String(w,0)+" واط");}}
    else if(text=="/help"||text=="/start"){
      bot.sendMessage(chatId,
                      "🤖 *Smart Billboard v6.0 ULTIMATE*\n━━━━━━━━━━━\n"
                      "⚡ /on /off /auto /toggle\n🚨 /emergency ⏱️ /timer N\n🔒 /lock /unlock\n"
                      "💻 /updatedash — تحديث لوحة التحكم من GitHub\n"
                      "━━━━━━━━━━━\n"
                      "📊 /stats /setwatts /resetstats\n"
                      "⏰ /setstart /setend /timezone\n🖥️ /nextpage /page N\n"
                      "━━━━━━━━━━━\n"
                      "📡 /network — BLE+WS+mDNS+NOW\n"
                      "💾 /backup — نسخة احتياطية\n"
                      "🔄 /checkupdate /selfupdate\n"
                      "🔐 /setpassword /login /logout\n"
                      "🔧 /ip /ota /log /status /reboot","Markdown");}
    else if (text == "/updatedash") {
      bot.sendMessage(chatId, "🔄 جاري تحديث لوحة التحكم من GitHub...");
      bool ok = fetchDashboardHTML();
      bot.sendMessage(chatId, ok ? "✅ تم تحديث لوحة التحكم بنجاح!" : "❌ فشل تحديث لوحة التحكم");
    }
    else if(text.startsWith("/"))bot.sendMessage(chatId,"❓ /help");
    updateDisplay();
    #ifdef USE_WEBSOCKET
    wsBroadcast();
    #endif
  }
}

// ═══════════════════════════════════════════════════════
//  SETUP
// ═══════════════════════════════════════════════════════
void setup() {
  Serial.begin(115200);
  Serial.println(F("\n╔══════════════════════════════════════╗"));
  Serial.println(F("║  Smart Billboard v6.0 ULTIMATE       ║"));
  Serial.println(F("║  Self-Contained Edition              ║"));
  Serial.println(F("╚══════════════════════════════════════╝\n"));

  pinMode(Config::RELAY_PIN,OUTPUT); digitalWrite(Config::RELAY_PIN,HIGH);
  pinMode(Config::STATUS_LED,OUTPUT); digitalWrite(Config::STATUS_LED,LOW);
  pinMode(Config::BUTTON_PIN,INPUT_PULLUP);

  Wire.begin(21,22);
  display.begin(); display.enableUTF8Print();
  sys.oledContrast=Config::CONTRAST_DAY; display.setContrast(sys.oledContrast);

  animatedSplash();

  #ifdef USE_LITTLEFS
  initFS();
  loadConfigJSON();
  loadScheduleJSON();
  loadUsersJSON();
  if (!FS_IMPL.exists("/dashboard.html")) {
    addLog("No dashboard file, fetching from GitHub...");
    fetchDashboardHTML();
  #else
  prefs.begin("billboard",true);
  sys.startHour=prefs.getInt("startHour",Config::DEFAULT_START);
  sys.endHour  =prefs.getInt("endHour",Config::DEFAULT_END);
  sys.tzOffset =prefs.getInt("tzOffset",Config::DEFAULT_TZ*3600);
  adminPassword=prefs.getString("adminPass","");
  stats.totalOnSec=prefs.getULong("totOn",0);
  stats.powerCycles=prefs.getInt("cycles",0);
  stats.estimatedKWh=prefs.getFloat("kWh",0.0);
  prefs.end();
  #endif

  sys.bootTime=millis();sys.fwVersion="v6.0-ULTIMATE";
  sys.manualOverride=sys.relayOn=sys.otaInProgress=sys.emergencyMode=sys.controlLocked=false;
  sys.captivePortalActive=sys.timerActive=sys.updateAvailable=false;
  sys.displayPage=0;sys.msgCount=0;sys.connectedNetIdx=-1;
  sys.peakOnHours=0;sys.lastGhCheck="";sys.updateVersion="";

  WiFi.setHostname("SmartBillboard");WiFi.mode(WIFI_STA);
  for(int n=0;n<3&&sys.connectedNetIdx<0;n++){
    if(!WIFI_NETS[n].ssid||strlen(WIFI_NETS[n].ssid)==0)continue;
    WiFi.begin(WIFI_NETS[n].ssid,WIFI_NETS[n].pass);
    for(int a=0;a<20&&WiFi.status()!=WL_CONNECTED;a++)delay(500);
    if(WiFi.status()==WL_CONNECTED){sys.wifiConnected=true;sys.connectedNetIdx=n;addLog("WiFi: "+String(WIFI_NETS[n].ssid)+" "+WiFi.localIP().toString());}
    else{WiFi.disconnect();delay(200);}
  }
  if(!sys.wifiConnected){
    errorScreen("WiFi Failed","AP Mode...");
    #ifdef USE_CAPTIVE_PORTAL
    startCaptivePortal();return;
    #endif
  }

  configTime(sys.tzOffset,0,"pool.ntp.org","time.nist.gov");
  for(int i=0;i<15;i++){struct tm t;if(getLocalTime(&t)){addLog("NTP: "+getTimeStr());break;}delay(400);}

  secureClient.setInsecure();

  #ifdef USE_MDNS
  if(MDNS.begin(Config::MDNS_NAME)){MDNS.addService("http","tcp",Config::WEB_PORT);addLog("mDNS: "+String(Config::MDNS_NAME)+".local");}
  #endif

  registerAPIRoutes();

  #ifdef USE_WEBSOCKET
  wsServer.begin();wsServer.onEvent(wsEvent);addLog("WS: :81");
  #endif

  #ifdef USE_BLE
  initBLE();
  #endif

  mqtt.setServer(mqttServer.c_str(),mqttPort);mqtt.setCallback(mqttCallback);
  if(mqttServer.length()>0)mqttReconnect();

  #ifdef USE_ESPNOW
  initESPNow();
  #endif

  #ifdef USE_DHT_SENSOR
  dht.begin();
  #endif

  esp_task_wdt_config_t wdt={.timeout_ms=Config::WDT_S*1000,.idle_core_mask=0,.trigger_panic=true};
  esp_task_wdt_init(&wdt);

  bot.longPoll=10;

  String welcome="🚀 *Smart Billboard v6.0 ULTIMATE*\n━━━━━━━━━━━\n";
  welcome+="📶 `"+WiFi.SSID()+"`\n🌐 `"+WiFi.localIP().toString()+"`\n";
  welcome+="🔧 `http://"+WiFi.localIP().toString()+":"+String(Config::WEB_PORT)+"/`\n";
  #ifdef USE_MDNS
  welcome+="🔗 `http://"+String(Config::MDNS_NAME)+".local/`\n";
  #endif
  #ifdef USE_BLE
  welcome+="📱 BLE: `"+String(Config::BLE_NAME)+"`\n";
  #endif
  welcome+="━━━━━━━━━━━\n✨ *الجديد في v6.0:*\n• Dashboard احترافي + Chart.js\n• File Manager + Code Editor\n• Backup/Restore كامل\n• نظام أمان متعدد المستويات\n/help للقائمة";
  bot.sendMessage(Config::ALLOWED_CHATS[0],welcome,"Markdown");
  audit("system","startup",WiFi.localIP().toString());

  checkDailyReset();updateRelay();updateDisplay();
  Serial.println(F("\n═══ v6.0 ULTIMATE Ready ═══\n"));
  }
}
// ═══════════════════════════════════════════════════════
//  LOOP
// ═══════════════════════════════════════════════════════
void loop(){
  esp_task_wdt_reset();
  #ifdef USE_CAPTIVE_PORTAL
  if(sys.captivePortalActive){handleCaptivePortal();SmartButton::update();return;}
  #endif
  if(WiFi.status()!=WL_CONNECTED){
    sys.wifiConnected=false;
    for(int n=0;n<3;n++){
      if(!WIFI_NETS[n].ssid||strlen(WIFI_NETS[n].ssid)==0)continue;
      WiFi.begin(WIFI_NETS[n].ssid,WIFI_NETS[n].pass);
      for(int a=0;a<10&&WiFi.status()!=WL_CONNECTED;a++)delay(500);
      if(WiFi.status()==WL_CONNECTED){sys.wifiConnected=true;sys.connectedNetIdx=n;addLog("إعادة اتصال: "+String(WIFI_NETS[n].ssid));if(mqttServer.length()>0)mqttReconnect();break;}
      WiFi.disconnect();delay(200);
    }
    if(!sys.wifiConnected){delay(3000);SmartButton::update();return;}
  }
  sys.wifiConnected=true;
  unsigned long now=millis();

  webServer.handleClient();
  ElegantOTA.loop();
  if(sys.otaInProgress){SmartButton::update();return;}
  #ifdef USE_WEBSOCKET
  wsServer.loop();
  if(now-sys.lastWsPush>=Config::WS_PUSH_MS){sys.lastWsPush=now;wsBroadcast();}
  #ifdef USE_BLE
  updateBLEStatus();
  #endif
  #endif
  if(mqttServer.length()>0){if(!mqtt.connected())mqttReconnect();mqtt.loop();
    if(now-sys.lastMqttPub>=60000){sys.lastMqttPub=now;if(mqtt.connected())mqtt.publish(mqttTopicState.c_str(),buildStatusJson().c_str(),true);}
  }
  SmartButton::update();
  if(now-sys.lastBotCheck>=Config::BOT_POLL_MS){
    sys.lastBotCheck=now;
    int n=bot.getUpdates(bot.last_message_received+1);
    while(n){handleMessages(n);n=bot.getUpdates(bot.last_message_received+1);}
  }
  if(now-sys.lastRelayCheck>=(unsigned long)Config::RELAY_CHK_S*1000){sys.lastRelayCheck=now;updateRelay();checkDailyReset();}
  #ifdef USE_DHT_SENSOR
  if(now-sys.lastSensorRead>=60000){sys.lastSensorRead=now;
    float h=dht.readHumidity(),tc=dht.readTemperature();
    if(!isnan(h)&&!isnan(tc)){humidity=h;temperature=tc;sensorOK=true;
      if(tc>=Config::TEMP_ALERT_H&&!tempAlertSent){bot.sendMessage(Config::ALLOWED_CHATS[0],"🌡️ *حرارة "+String(tc,1)+"°C*","Markdown");tempAlertSent=true;}
      else if(tc<Config::TEMP_ALERT_H-3)tempAlertSent=false;
    } else sensorOK=false;
  }
  #endif
  if(now-sys.lastStatsSave>=(unsigned long)Config::STATS_SAVE_S*1000){
    sys.lastStatsSave=now;
    if(sys.relayOn&&stats.lastOnTime>0){
      unsigned long ex=(now-stats.lastOnTime)/1000;
      stats.totalOnSec+=ex;stats.todayOnSec+=ex;
      stats.estimatedKWh+=(LOAD_WATTS*ex)/3600000.0;
      if(stats.todayOnSec/3600.0>sys.peakOnHours)sys.peakOnHours=stats.todayOnSec/3600.0;
      stats.lastOnTime=now;
    }
    saveConfigJSON();
    #ifdef USE_SELF_UPDATE
    static unsigned long lastGhCheck=0;
    if(now-lastGhCheck>24UL*3600*1000){lastGhCheck=now;checkGitHubUpdate();}
    #endif
  }
  if(now-sys.lastDisplayUpd>=(unsigned long)Config::DISPLAY_UPD_S*1000){
    sys.lastDisplayUpd=now;
    if(!sys.controlLocked&&!sys.emergencyMode)sys.displayPage++;
    updateDisplay();
  }
  tokens.erase(std::remove_if(tokens.begin(),tokens.end(),[&](const AuthToken&t){return millis()>t.expiry;}),tokens.end());
}
