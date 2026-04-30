//#define USE_BLE
#define USE_WEBSOCKET
//#define USE_MDNS
#define USE_CAPTIVE_PORTAL
//#define USE_ESPNOW
#define USE_LITTLEFS
//#define USE_SELF_UPDATE
//#define USE_ADVANCED_SCHEDULE
//#define USE_RATE_LIMIT
//#define USE_AUDIT_LOG
// #define USE_DHT_SENSOR
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
void espNowBroadcast();
#endif

#ifdef USE_DHT_SENSOR
#include <DHT.h>
DHT dht(14, DHT22);
float temperature=0, humidity=0;
bool sensorOK=false, tempAlertSent=false;
#endif

struct WiFiCred { const char* ssid; const char* pass; };
WiFiCred WIFI_NETS[3] = {
  { "000", "000" },
  { "2700",                 "11199777"       },
  { "000",  "000"     },
};

namespace Config {
  constexpr char BOT_TOKEN[]    = "000";
  const String   ALLOWED_CHATS[]= {"000"};
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
  constexpr int  RELAY_PIN      = 25;
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
  constexpr int  CONTRAST_NIGHT = 50;
  constexpr int  CONTRAST_DAY   = 255;
  constexpr float TEMP_ALERT_H  = 45.0;
  constexpr int  RATE_LIMIT_REQ = 30;
  constexpr int  RATE_WINDOW_MS = 60000;
}

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

WiFiClientSecure secureClient;
WiFiClient       plainClient;
UniversalTelegramBot bot(Config::BOT_TOKEN, secureClient);
WebServer        webServer(Config::WEB_PORT);
Preferences      prefs;
U8G2_SSD1306_128X64_NONAME_1_HW_I2C display(U8G2_R0, U8X8_PIN_NONE);
WiFiClient       mqttWiFiClient;
PubSubClient     mqtt(mqttWiFiClient);
DNSServer        dnsServer;

#define LOG_SIZE 50
struct LogEntry { uint32_t ts; String msg; };
LogEntry eventLog[LOG_SIZE];
int logIdx=0;

void addLog(const String& msg) {
  eventLog[logIdx%LOG_SIZE]={millis(),msg};
  logIdx++;
  Serial.println("[LOG] "+msg);
}

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

String mqttServer=Config::MQTT_SRV, mqttTopicCmd=Config::MQTT_CMD, mqttTopicState=Config::MQTT_STATE;
String mqttUser=Config::MQTT_USER, mqttPassStr=Config::MQTT_PASS, mqttClientId=Config::MQTT_CID;
int    mqttPort=Config::MQTT_PORT;
int    nightDimStart=Config::DIM_START, nightDimEnd=Config::DIM_END;
int    nightContrast=Config::CONTRAST_NIGHT, dayContrast=Config::CONTRAST_DAY;

String adminPassword="";
std::vector<String> authenticatedChats;

String getTimeStr();
void setRelay(bool on);
bool shouldBeOn();
String buildStatusJson();
void serveDashboard();
String buildDashboardHTML();
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

#endif // USE_LITTLEFS

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

#ifdef USE_ESPNOW
void onESPNowRx(const esp_now_recv_info_t* info, const uint8_t* d, int l){
  if(l!=sizeof(ESPNowMsg))return;
  memcpy(&espNowRx,d,sizeof(espNowRx));
  addLog("ESP-NOW cmd="+String(espNowRx.cmd));
  if(espNowRx.cmd==0){sys.manualOverride=true;sys.manualState=false;setRelay(false);}
  else if(espNowRx.cmd==1){sys.manualOverride=true;sys.manualState=true;setRelay(true);}
  else if(espNowRx.cmd==2){sys.manualOverride=true;sys.manualState=!sys.relayOn;setRelay(sys.manualState);}
}

void onESPNowTx(const uint8_t* mac_addr, esp_now_send_status_t status){}

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
      else if (cmd == "stoptimer") {
        sys.timerActive = false;
        sys.manualOverride = false;
        updateRelay();
      }
      else if (cmd.startsWith("advsched:")) {
        sys.useAdvancedSchedule = (cmd == "advsched:on");
        saveConfigJSON();
        updateRelay();
      }
      else if (cmd == "ping") {
        // keep-alive, لا فعل مطلوب
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
    char sch[28];sprintf(sch,"SCHED %02d:00->%02d:00",sys.startHour,sys.endHour==0?24:sys.endHour);
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
    #ifdef USE_LITTLEFS
    sprintf(b,"FS: %luKB/%luKB",(unsigned long)FS_IMPL.usedBytes()/1024,(unsigned long)FS_IMPL.totalBytes()/1024);
    display.setCursor(0,54);display.print(b);
    #endif
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
      #else
      display.setCursor(0,34);display.print("Advanced OFF");
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
  digitalWrite(Config::RELAY_PIN, on ? HIGH : LOW);
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
  if(vl){
    blink(5);addLog("زر: Captive Portal");
    #ifdef USE_CAPTIVE_PORTAL
    startCaptivePortal();             // هنا يجب أن يكون startCaptivePortal فقط
    #endif
    return;
  }
  if(lp){
    sys.controlLocked=!sys.controlLocked;blink(sys.controlLocked?3:1);
    audit("button","lock:"+(String)(sys.controlLocked?"on":"off"));
    addLog("زر: "+(String)(sys.controlLocked?"قفل":"فتح"));
    updateDisplay();return;
  }
  if(sys.controlLocked){blink(1);return;}
  if(clicks>=3){
    sys.emergencyMode=!sys.emergencyMode;blink(3);
    if(sys.emergencyMode){
      sys.manualOverride=true;sys.manualState=true;setRelay(true);addLog("زر: طارئ ON");
      bot.sendMessage(Config::ALLOWED_CHATS[0],"🚨 *وضع الطارئ!* تشغيل قسري\n/auto للإلغاء","Markdown");
    } else {
      sys.manualOverride=false;updateRelay();addLog("زر: إلغاء الطارئ");
      bot.sendMessage(Config::ALLOWED_CHATS[0],"✅ تم إلغاء وضع الطارئ");
    }
    audit("button","emergency:"+(String)sys.emergencyMode);updateDisplay();return;
  }
  if(clicks==2){
    sys.displayPage++;blink(2);
    addLog("زر: صفحة "+String(sys.displayPage%5));
    updateDisplay();return;
  }
  if(clicks==1){
    if(sys.emergencyMode)return;
    sys.manualOverride=true;sys.manualState=!sys.relayOn;sys.timerActive=false;
    setRelay(sys.manualState);blink(1);
    audit("button","toggle:"+(String)sys.manualState);
    addLog("زر: "+(String)(sys.manualState?"ON":"OFF"));
    updateDisplay();
    #ifdef USE_WEBSOCKET
    wsBroadcast();
    #endif
    #ifdef USE_BLE
    updateBLEStatus();
    #endif
  }
}

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

void serveDashboard() {
  #ifdef USE_LITTLEFS
  if (FS_IMPL.exists("/dashboard.html")) {
    File f = FS_IMPL.open("/dashboard.html", "r");
    if (f) {
      webServer.streamFile(f, "text/html; charset=utf-8");
      f.close();
      return;
    }
  }
  #endif
  // fallback: الصفحة المضمنة
  webServer.send(200, "text/html; charset=utf-8", buildDashboardHTML());
}

String buildDashboardHTML() {
  String wsPort = String(Config::WS_PORT);
  String webPort = String(Config::WEB_PORT);

  String html = R"HTML(<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Smart Billboard v6.0 ULTIMATE</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Tajawal:wght@300;400;700;900&display=swap');
:root{
  --bg:#060810;--bg2:#0d1120;--bg3:#141a2e;--bg4:#1c2340;
  --border:#1e2a4a;--border2:#2a3a5e;
  --accent:#00d4ff;--accent2:#0088aa;--accent3:#004466;
  --green:#00ff88;--green2:#00aa55;
  --red:#ff3366;--red2:#aa1133;
  --yellow:#ffcc00;--orange:#ff8800;
  --text:#e8edf5;--text2:#8899bb;--text3:#445577;
  --mono:'IBM Plex Mono',monospace;--sans:'Tajawal',sans-serif;
  --radius:6px;--shadow:0 4px 24px rgba(0,0,0,.5);
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:var(--sans)}
.shell{display:grid;grid-template-rows:52px 1fr;grid-template-columns:220px 1fr;height:100vh;overflow:hidden}
.topbar{grid-column:1/-1;display:flex;align-items:center;gap:12px;padding:0 20px;
        background:var(--bg2);border-bottom:1px solid var(--border);z-index:100}
.sidebar{background:var(--bg2);border-left:1px solid var(--border);padding:16px 0;overflow-y:auto;
  display:flex;flex-direction:column;gap:2px}
  .main{overflow-y:auto;padding:20px}
  .logo{font-family:var(--mono);font-size:.85em;font-weight:600;color:var(--accent);letter-spacing:.1em}
  .logo span{color:var(--text2)}
  .status-pill{display:flex;align-items:center;gap:6px;padding:4px 12px;border-radius:20px;
    font-size:.72em;font-family:var(--mono);font-weight:600;letter-spacing:.05em}
    .pill-on{background:rgba(0,255,136,.1);color:var(--green);border:1px solid var(--green2)}
    .pill-off{background:rgba(255,51,102,.1);color:var(--red);border:1px solid var(--red2)}
    .pill-em{background:rgba(255,136,0,.15);color:var(--orange);border:1px solid var(--orange);animation:pulse 1s infinite}
    .ws-badge{font-size:.68em;font-family:var(--mono);padding:3px 10px;border-radius:12px}
    .ws-ok{background:rgba(0,212,255,.1);color:var(--accent);border:1px solid var(--accent2)}
    .ws-err{background:rgba(255,51,102,.08);color:var(--red);border:1px solid var(--red2)}
    @keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
    .ml-auto{margin-inline-start:auto}
    .nav-item{display:flex;align-items:center;gap:10px;padding:10px 20px;cursor:pointer;
      color:var(--text2);font-size:.85em;transition:all .15s;border-right:3px solid transparent}
      .nav-item:hover{background:var(--bg3);color:var(--text)}
      .nav-item.active{background:var(--bg3);color:var(--accent);border-right-color:var(--accent)}
      .nav-icon{font-size:1.1em;width:20px;text-align:center}
      .nav-label{flex:1}
      .nav-badge{background:var(--red);color:#fff;border-radius:10px;padding:1px 7px;font-size:.7em;font-family:var(--mono)}
      .nav-sep{height:1px;background:var(--border);margin:8px 16px}
      .page{display:none}.page.active{display:block}
      .kpi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:20px}
      .kpi{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:16px;
        position:relative;overflow:hidden;transition:border-color .2s}
        .kpi:hover{border-color:var(--border2)}
        .kpi::before{content:'';position:absolute;inset:0;background:linear-gradient(135deg,var(--accent3)0%,transparent 60%);opacity:.15}
        .kpi-label{font-size:.68em;color:var(--text2);text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;font-family:var(--mono)}
        .kpi-val{font-size:1.8em;font-weight:900;font-family:var(--mono);line-height:1}
        .kpi-sub{font-size:.7em;color:var(--text2);margin-top:4px}
        .kpi-val.on{color:var(--green)}.kpi-val.off{color:var(--red)}.kpi-val.info{color:var(--accent)}
        .kpi-val.warn{color:var(--yellow)}
        .card{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:16px;overflow:hidden}
        .card-hdr{padding:12px 16px;border-bottom:1px solid var(--border);font-size:.8em;font-family:var(--mono);
          color:var(--text2);text-transform:uppercase;letter-spacing:.08em;display:flex;align-items:center;gap:8px}
          .card-hdr span{color:var(--accent)}
          .card-body{padding:16px}
          .btns{display:flex;flex-wrap:wrap;gap:8px}
          .btn{padding:9px 18px;border-radius:var(--radius);border:1px solid var(--border2);
            background:var(--bg3);color:var(--text);cursor:pointer;font-size:.82em;font-family:var(--mono);
            font-weight:600;transition:all .15s;letter-spacing:.03em}
            .btn:hover{transform:translateY(-1px);box-shadow:var(--shadow)}
            .btn-on{border-color:var(--green2);color:var(--green)}.btn-on:hover{background:rgba(0,255,136,.1)}
            .btn-off{border-color:var(--red2);color:var(--red)}.btn-off:hover{background:rgba(255,51,102,.1)}
            .btn-em{border-color:var(--orange);color:var(--orange)}.btn-em:hover{background:rgba(255,136,0,.1)}
            .btn-info{border-color:var(--accent2);color:var(--accent)}.btn-info:hover{background:rgba(0,212,255,.1)}
            .btn-warn{border-color:var(--yellow);color:var(--yellow)}.btn-warn:hover{background:rgba(255,204,0,.1)}
            .btn-sm{padding:6px 12px;font-size:.75em}
            .btn-danger{border-color:var(--red2);color:var(--red)}.btn-danger:hover{background:rgba(255,51,102,.1)}
            .timer-row{display:flex;align-items:center;gap:8px;margin-top:12px}
            .inp{padding:8px 12px;border-radius:var(--radius);border:1px solid var(--border2);background:var(--bg3);
              color:var(--text);font-family:var(--mono);font-size:.85em}
              .inp:focus{outline:none;border-color:var(--accent)}
              .chart-wrap{position:relative;height:180px}
              .log-wrap{max-height:260px;overflow-y:auto;font-family:var(--mono);font-size:.75em}
              .log-entry{padding:5px 0;border-bottom:1px solid var(--border);display:flex;gap:8px;align-items:flex-start}
              .log-ts{color:var(--text3);white-space:nowrap;min-width:60px}
              .log-msg{color:var(--text2);flex:1;word-break:break-all}
              .log-filter{display:flex;gap:8px;margin-bottom:10px}
              .file-list{font-family:var(--mono);font-size:.8em}
              .file-row{display:flex;align-items:center;gap:12px;padding:8px 0;border-bottom:1px solid var(--border)}
              .file-name{flex:1;color:var(--accent)}.file-size{color:var(--text2);min-width:60px;text-align:end}
              .file-acts{display:flex;gap:6px}
              .editor-wrap{position:relative}
              .code-editor{width:100%;min-height:320px;padding:14px;
                background:var(--bg);border:1px solid var(--border2);border-radius:var(--radius);
                color:#a8d8ea;font-family:var(--mono);font-size:.82em;line-height:1.6;
                resize:vertical;tab-size:2;outline:none}
                .code-editor:focus{border-color:var(--accent)}
                .editor-toolbar{display:flex;gap:8px;margin-bottom:10px;align-items:center}
                .editor-fname{flex:1;color:var(--text2);font-family:var(--mono);font-size:.85em}
                .rule-card{background:var(--bg3);border:1px solid var(--border);border-radius:var(--radius);padding:14px;margin-bottom:10px}
                .rule-hdr{display:flex;align-items:center;gap:10px;margin-bottom:10px}
                .rule-name{flex:1;font-weight:700;font-family:var(--mono)}
                .rule-time{font-size:1.1em;color:var(--accent);font-family:var(--mono);font-weight:600}
                .badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.7em;font-family:var(--mono)}
                .badge-on{background:rgba(0,255,136,.15);color:var(--green)}
                .badge-off{background:rgba(255,51,102,.15);color:var(--red)}
                .badge-type{background:var(--bg4);color:var(--text2)}
                .user-row{display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid var(--border)}
                .user-name{flex:1;font-family:var(--mono);font-weight:600}
                .role-badge{font-size:.7em;padding:2px 8px;border-radius:10px;font-family:var(--mono)}
                .role-admin{background:rgba(255,204,0,.15);color:var(--yellow)}
                .role-op{background:rgba(0,212,255,.15);color:var(--accent)}
                .role-view{background:var(--bg4);color:var(--text2)}
                .upload-zone{border:2px dashed var(--border2);border-radius:var(--radius);padding:40px;
                  text-align:center;cursor:pointer;transition:all .2s;color:var(--text2)}
                  .upload-zone:hover,.upload-zone.drag{border-color:var(--accent);color:var(--accent);background:rgba(0,212,255,.05)}
                  .progress-bar{height:6px;background:var(--bg3);border-radius:3px;overflow:hidden;margin-top:12px;display:none}
                  .progress-fill{height:100%;background:linear-gradient(90deg,var(--accent2),var(--accent));width:0%;transition:width .3s}
                  .modal-bg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:1000;align-items:center;justify-content:center}
                  .modal-bg.open{display:flex}
                  .modal{background:var(--bg2);border:1px solid var(--border2);border-radius:8px;padding:24px;width:90%;max-width:480px;max-height:90vh;overflow-y:auto}
                  .modal-hdr{font-family:var(--mono);font-weight:600;color:var(--accent);margin-bottom:16px;font-size:.95em}
                  .form-row{margin-bottom:12px}
                  .form-label{display:block;font-size:.75em;color:var(--text2);margin-bottom:4px;font-family:var(--mono)}
                  .form-inp{width:100%;padding:9px 12px;border-radius:var(--radius);border:1px solid var(--border2);
                    background:var(--bg3);color:var(--text);font-family:var(--mono);font-size:.85em}
                    .form-inp:focus{outline:none;border-color:var(--accent)}
                    ::-webkit-scrollbar{width:4px;height:4px}
                    ::-webkit-scrollbar-track{background:var(--bg)}
                    ::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
                    @media(max-width:768px){
                      .shell{grid-template-columns:1fr;grid-template-rows:52px auto 1fr}
                      .sidebar{grid-row:2;flex-direction:row;overflow-x:auto;padding:8px;border-left:none;border-bottom:1px solid var(--border)}
                      .nav-item{flex-direction:column;gap:4px;padding:8px 12px;font-size:.7em;border-right:none;border-bottom:3px solid transparent}
                      .nav-item.active{border-bottom-color:var(--accent);border-right-color:transparent}
                      .nav-label{display:none}
                    }
                    </style>
                    </head>
                    <body>
                    <div class="shell">
                    <header class="topbar">
                    <div class="logo">SMART<span>/</span>BILLBOARD <span>v6.0</span></div>
                    <div id="relay-pill" class="status-pill pill-off" style="margin-right:8px">● OFFLINE</div>
                    <div id="mode-pill" class="status-pill" style="background:rgba(0,212,255,.08);color:var(--accent);border:1px solid var(--accent2)">AUTO</div>
                    <div class="ml-auto" style="display:flex;align-items:center;gap:10px">
                    <div id="ws-badge" class="ws-badge ws-err">⬤ DISCONNECTED</div>
                    <div style="font-family:var(--mono);font-size:.75em;color:var(--text2)" id="hdr-time">--:--</div>
                    </div>
                    </header>

                    <nav class="sidebar">
                    <div class="nav-item active" onclick="showPage('dashboard',this)"><span class="nav-icon">⬡</span><span class="nav-label">Dashboard</span></div>
                    <div class="nav-item" onclick="showPage('schedule',this)"><span class="nav-icon">◷</span><span class="nav-label">الجدول</span></div>
                    <div class="nav-item" onclick="showPage('files',this)"><span class="nav-icon">◈</span><span class="nav-label">الملفات</span></div>
                    <div class="nav-item" onclick="showPage('editor',this)"><span class="nav-icon">⊞</span><span class="nav-label">المحرر</span></div>
                    <div class="nav-item" onclick="showPage('ota',this)"><span class="nav-icon">↑</span><span class="nav-label">التحديث</span>
                    <span class="nav-badge" id="ota-badge" style="display:none">!</span>
                    </div>
                    <div class="nav-sep"></div>
                    <div class="nav-item" onclick="showPage('security',this)"><span class="nav-icon">◉</span><span class="nav-label">الأمان</span></div>
                    <div class="nav-item" onclick="showPage('users',this)"><span class="nav-icon">◎</span><span class="nav-label">المستخدمون</span></div>
                    <div class="nav-sep"></div>
                    <div class="nav-item" onclick="showPage('log',this)"><span class="nav-icon">≡</span><span class="nav-label">السجل</span></div>
                    <div class="nav-item" onclick="showPage('system',this)"><span class="nav-icon">⊕</span><span class="nav-label">النظام</span></div>
                    </nav>

                    <main class="main">

                    <!-- Dashboard -->
                    <div id="page-dashboard" class="page active">
                    <div class="kpi-grid">
                    <div class="kpi"><div class="kpi-label">الحالة</div><div class="kpi-val off" id="k-state">إطفاء</div><div class="kpi-sub" id="k-mode">تلقائي</div></div>
                    <div class="kpi"><div class="kpi-label">الوقت</div><div class="kpi-val info" id="k-time">--:--</div><div class="kpi-sub" id="k-date">----</div></div>
                    <div class="kpi"><div class="kpi-label">تشغيل اليوم</div><div class="kpi-val" id="k-today">0.00h</div><div class="kpi-sub" id="k-peak">ذروة: 0h</div></div>
                    <div class="kpi"><div class="kpi-label">الطاقة المستهلكة</div><div class="kpi-val info" id="k-kwh">0.000</div><div class="kpi-sub">kWh</div></div>
                    <div class="kpi"><div class="kpi-label">إشارة WiFi</div><div class="kpi-val" id="k-rssi">---</div><div class="kpi-sub" id="k-ssid">---</div></div>
                    <div class="kpi"><div class="kpi-label">ذاكرة حرة</div><div class="kpi-val info" id="k-heap">---</div><div class="kpi-sub">KB RAM</div></div>
                    <div class="kpi"><div class="kpi-label">إجمالي التشغيل</div><div class="kpi-val" id="k-total">0.0h</div><div class="kpi-sub" id="k-cycles">0 دورة</div></div>
                    <div class="kpi"><div class="kpi-label">وقت التشغيل</div><div class="kpi-val info" id="k-uptime">---</div><div class="kpi-sub" id="k-fs">FS ---</div></div>
                    </div>
                    <div class="card">
                    <div class="card-hdr"><span>⚡</span> التحكم السريع</div>
                    <div class="card-body">
                    <div class="btns">
                    <button class="btn btn-on" onclick="send('on')">■ تشغيل</button>
                    <button class="btn btn-off" onclick="send('off')">■ إطفاء</button>
                    <button class="btn btn-info" onclick="send('auto')">◌ تلقائي</button>
                    <button class="btn" onclick="send('toggle')">⇄ تبديل</button>
                    <button class="btn btn-em" onclick="send('emergency')">⚠ طارئ</button>
                    <button class="btn" onclick="send('nextpage')">◫ شاشة</button>
                    </div>
                    <div class="timer-row">
                    <span style="font-size:.8em;color:var(--text2);font-family:var(--mono)">مؤقت:</span>
                    <input class="inp" id="t-min" type="number" min="1" max="1440" placeholder="دقائق" style="width:90px">
                    <button class="btn btn-info btn-sm" onclick="sendTimer()">▶ تشغيل</button>
                    <button class="btn btn-sm" onclick="send('stoptimer')">■ إلغاء</button>
                    </div>
                    </div>
                    </div>
                    <div class="card">
                    <div class="card-hdr"><span>◈</span> ساعات التشغيل — آخر 7 أيام</div>
                    <div class="card-body"><div class="chart-wrap"><canvas id="weekly-chart"></canvas></div></div>
                    </div>
                    </div>

                    <!-- Schedule -->
                    <div id="page-schedule" class="page">
                    <div class="card">
                    <div class="card-hdr"><span>◷</span> الجدول الزمني
                    <button class="btn btn-info btn-sm" style="margin-right:auto" onclick="openRuleModal()">+ قاعدة جديدة</button>
                    </div>
                    <div class="card-body">
                    <div class="btns" style="margin-bottom:16px">
                    <button class="btn btn-sm" onclick="setSchedMode('simple')">◌ بسيط</button>
                    <button class="btn btn-sm btn-info" onclick="setSchedMode('advanced')">⊞ متقدم</button>
                    </div>
                    <div id="simple-sched" style="display:none">
                    <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;font-family:var(--mono);font-size:.85em">
                    <label style="color:var(--text2)">بدء:</label>
                    <input class="inp" id="sh" type="number" min="0" max="23" style="width:70px">
                    <label style="color:var(--text2)">نهاية:</label>
                    <input class="inp" id="eh" type="number" min="0" max="24" style="width:70px">
                    <button class="btn btn-info btn-sm" onclick="saveSimpleSched()">حفظ</button>
                    </div>
                    </div>
                    <div id="rules-list"></div>
                    </div>
                    </div>
                    </div>

                    <!-- File Manager -->
                    <div id="page-files" class="page">
                    <div class="card">
                    <div class="card-hdr"><span>◈</span> إدارة الملفات (LittleFS)
                    <button class="btn btn-info btn-sm" style="margin-right:auto" onclick="refreshFiles()">↻ تحديث</button>
                    </div>
                    <div class="card-body">
                    <div style="display:flex;gap:12px;margin-bottom:16px;font-family:var(--mono);font-size:.8em;color:var(--text2)">
                    <span>المساحة: <span id="fs-used" style="color:var(--accent)">---</span> / <span id="fs-total">---</span> KB</span>
                    <div style="flex:1;height:6px;background:var(--bg3);border-radius:3px;align-self:center">
                    <div id="fs-bar" style="height:100%;background:linear-gradient(90deg,var(--accent2),var(--accent));width:0%;border-radius:3px;transition:.3s"></div>
                    </div>
                    </div>
                    <div class="file-list" id="file-list"></div>
                    <div style="margin-top:16px">
                    <input type="file" id="upload-file" style="display:none" onchange="uploadFile(this)">
                    <button class="btn btn-info btn-sm" onclick="document.getElementById('upload-file').click()">↑ رفع ملف</button>
                    <button class="btn btn-sm" onclick="backupDownload()">↓ تصدير Backup</button>
                    <label class="btn btn-sm" style="cursor:pointer">↑ استيراد Backup
                    <input type="file" accept=".json" style="display:none" onchange="restoreBackup(this)">
                    </label>
                    </div>
                    </div>
                    </div>
                    </div>

                    <!-- Code Editor -->
                    <div id="page-editor" class="page">
                    <div class="card">
                    <div class="card-hdr"><span>⊞</span> محرر الإعدادات</div>
                    <div class="card-body">
                    <div class="editor-toolbar">
                    <select class="inp" id="edit-file" onchange="loadFileEditor(this.value)" style="width:auto">
                    <option value="/config.json">config.json</option>
                    <option value="/schedule.json">schedule.json</option>
                    <option value="/users.json">users.json</option>
                    <option value="/audit.log">audit.log</option>
                    </select>
                    <span class="editor-fname" id="edit-name">/config.json</span>
                    <button class="btn btn-info btn-sm" onclick="saveFileEditor()">💾 حفظ</button>
                    <button class="btn btn-sm" onclick="formatJSON()">{ } تنسيق</button>
                    <button class="btn btn-warn btn-sm" onclick="loadFileEditor(document.getElementById('edit-file').value)">↻ إعادة تحميل</button>
                    </div>
                    <div class="editor-wrap">
                    <textarea class="code-editor" id="code-editor" spellcheck="false"></textarea>
                    </div>
                    <div style="font-family:var(--mono);font-size:.72em;color:var(--text3);margin-top:8px" id="editor-status">جاهز</div>
                    </div>
                    </div>
                    </div>

                    <!-- OTA -->
                    <div id="page-ota" class="page">
                    <div class="card">
                    <div class="card-hdr"><span>↑</span> تحديث Firmware (OTA)</div>
                    <div class="card-body">
                    <div style="font-family:var(--mono);font-size:.82em;margin-bottom:16px">
                    <div>الإصدار الحالي: <span id="cur-fw" style="color:var(--accent)">---</span></div>
                    <div id="upd-avail" style="display:none;margin-top:8px;color:var(--green)">
                    ⬆ إصدار جديد متاح: <span id="new-fw-ver" style="color:var(--accent)">---</span>
                    </div>
                    </div>
                    <div class="upload-zone" id="drop-zone"
                    ondragover="event.preventDefault();this.classList.add('drag')"
                    ondragleave="this.classList.remove('drag')"
                    ondrop="handleDrop(event)"
                    onclick="document.getElementById('fw-file').click()">
                    <div style="font-size:2em;margin-bottom:8px">↑</div>
                    <div>اسحب ملف firmware.bin هنا أو انقر للاختيار</div>
                    <div style="font-size:.75em;color:var(--text3);margin-top:6px">يقبل: .bin</div>
                    </div>
                    <input type="file" id="fw-file" accept=".bin" style="display:none" onchange="uploadFirmware(this.files[0])">
                    <div class="progress-bar" id="fw-progress"><div class="progress-fill" id="fw-fill"></div></div>
                    <div id="fw-status" style="font-family:var(--mono);font-size:.8em;color:var(--text2);margin-top:10px;display:none"></div>
                    <div style="margin-top:16px;display:flex;gap:8px;flex-wrap:wrap">
                    <button class="btn btn-info btn-sm" onclick="checkGitHub()">⬡ فحص GitHub</button>
                    <button class="btn btn-sm" onclick="performSelfUpdate()">↑ تحديث من GitHub</button>
                    </div>
                    </div>
                    </div>
                    <div class="card">
                    <div class="card-hdr"><span>◌</span> OTA الكلاسيكي (ElegantOTA)</div>
                    <div class="card-body">
                    <a href="/update" target="_blank" class="btn btn-sm">فتح صفحة OTA ←</a>
                    </div>
                    </div>
                    </div>

                    <!-- Security -->
                    <div id="page-security" class="page">
                    <div class="card">
                    <div class="card-hdr"><span>◉</span> إعدادات الأمان</div>
                    <div class="card-body">
                    <div style="font-family:var(--mono);font-size:.82em;color:var(--text2);margin-bottom:16px">
                    Rate Limit: <span style="color:var(--accent)">30 طلب/دقيقة</span> &nbsp;|&nbsp;
                    Tokens نشطة: <span id="active-tokens" style="color:var(--accent)">---</span>
                    </div>
                    <div class="btns">
                    <button class="btn btn-sm btn-warn" onclick="revokeAllTokens()">✕ إلغاء كل الجلسات</button>
                    <button class="btn btn-sm" onclick="openChangePassModal()">🔑 تغيير كلمة المرور</button>
                    </div>
                    <div style="margin-top:16px">
                    <div class="form-row">
                    <label class="form-label">IP Whitelist (واحد لكل سطر)</label>
                    <textarea class="form-inp" id="ip-whitelist" rows="4" style="font-family:var(--mono);font-size:.8em;resize:vertical"></textarea>
                    </div>
                    <button class="btn btn-info btn-sm" onclick="saveWhitelist()">حفظ Whitelist</button>
                    </div>
                    <div style="margin-top:16px">
                    <div class="card-hdr" style="padding:0;margin-bottom:10px;border:none"><span>◉</span> سجل التدقيق</div>
                    <div class="log-wrap" id="audit-log-wrap" style="max-height:200px"></div>
                    <button class="btn btn-sm" style="margin-top:8px" onclick="loadAuditLog()">↻ تحديث</button>
                    </div>
                    </div>
                    </div>
                    </div>

                    <!-- Users -->
                    <div id="page-users" class="page">
                    <div class="card">
                    <div class="card-hdr"><span>◎</span> إدارة المستخدمين
                    <button class="btn btn-info btn-sm" style="margin-right:auto" onclick="openUserModal()">+ مستخدم جديد</button>
                    </div>
                    <div class="card-body">
                    <div id="users-list"></div>
                    </div>
                    </div>
                    </div>

                    <!-- Log -->
                    <div id="page-log" class="page">
                    <div class="card">
                    <div class="card-hdr"><span>≡</span> سجل الأحداث
                    <button class="btn btn-sm" style="margin-right:auto" onclick="clearLog()">مسح العرض</button>
                    </div>
                    <div class="card-body">
                    <div class="log-filter">
                    <input class="inp" id="log-search" placeholder="بحث..." oninput="filterLog()" style="flex:1;font-size:.8em">
                    <select class="inp" id="log-level" onchange="filterLog()" style="width:auto;font-size:.8em">
                    <option value="">الكل</option><option value="مرحل">مرحل</option>
                    <option value="زر">زر</option><option value="WiFi">WiFi</option>
                    <option value="BLE">BLE</option><option value="MQTT">MQTT</option>
                    </select>
                    </div>
                    <div class="log-wrap" id="main-log"></div>
                    </div>
                    </div>
                    </div>

                    <!-- System -->
                    <div id="page-system" class="page">
                    <div class="card">
                    <div class="card-hdr"><span>⊕</span> معلومات النظام</div>
                    <div class="card-body">
                    <div id="sys-info" style="font-family:var(--mono);font-size:.8em;line-height:2;color:var(--text2)">جاري التحميل...</div>
                    </div>
                    </div>
                    <div class="card">
                    <div class="card-hdr"><span>⊕</span> إعدادات MQTT</div>
                    <div class="card-body">
                    <div class="form-row"><label class="form-label">الخادم</label><input class="form-inp" id="mqtt-srv" placeholder="broker.mqtt.com"></div>
                    <div class="form-row"><label class="form-label">المنفذ</label><input class="form-inp" id="mqtt-port" type="number" value="1883"></div>
                    <div class="form-row"><label class="form-label">المستخدم</label><input class="form-inp" id="mqtt-user"></div>
                    <div class="form-row"><label class="form-label">كلمة المرور</label><input class="form-inp" id="mqtt-pass" type="password"></div>
                    <button class="btn btn-info btn-sm" onclick="saveMQTT()">حفظ MQTT</button>
                    </div>
                    </div>
                    <div class="card">
                    <div class="card-hdr"><span>⚠</span> عمليات النظام</div>
                    <div class="card-body">
                    <div class="btns">
                    <button class="btn btn-danger" onclick="if(confirm('إعادة تشغيل؟'))apiCmd('/cmd?action=reboot')">↻ إعادة تشغيل</button>
                    <button class="btn btn-danger" onclick="if(confirm('مسح الإحصائيات؟'))apiCmd('/cmd?action=resetstats')">◌ مسح الإحصائيات</button>
                    <button class="btn btn-warn" onclick="apiCmd('/cmd?action=saveconfig')">💾 حفظ الإعدادات</button>
                    </div>
                    </div>
                    </div>
                    </div>

                    </main>
                    </div>

                    <!-- Modals -->
                    <div class="modal-bg" id="rule-modal">
                    <div class="modal">
                    <div class="modal-hdr">+ قاعدة جدول جديدة</div>
                    <div class="form-row"><label class="form-label">الاسم</label><input class="form-inp" id="r-name" placeholder="اسم القاعدة"></div>
                    <div class="form-row">
                    <label class="form-label">النوع</label>
                    <select class="form-inp" id="r-type">
                    <option value="0">يومي</option><option value="1">أسبوعي</option>
                    <option value="2">تاريخ محدد</option><option value="3">موسمي</option>
                    </select>
                    </div>
                    <div class="form-row" style="display:flex;gap:12px">
                    <div style="flex:1"><label class="form-label">بدء</label>
                    <div style="display:flex;gap:4px">
                    <input class="form-inp" id="r-sh" type="number" min="0" max="23" value="19" style="width:60px">
                    :<input class="form-inp" id="r-sm" type="number" min="0" max="59" value="0" style="width:60px">
                    </div>
                    </div>
                    <div style="flex:1"><label class="form-label">نهاية</label>
                    <div style="display:flex;gap:4px">
                    <input class="form-inp" id="r-eh" type="number" min="0" max="24" value="0" style="width:60px">
                    :<input class="form-inp" id="r-em" type="number" min="0" max="59" value="0" style="width:60px">
                    </div>
                    </div>
                    </div>
                    <div class="form-row"><label class="form-label">الأولوية (1-10)</label><input class="form-inp" id="r-pri" type="number" min="1" max="10" value="1"></div>
                    <div class="form-row">
                    <label class="form-label">الحالة</label>
                    <select class="form-inp" id="r-force">
                    <option value="1">تشغيل قسري</option><option value="0">إيقاف قسري</option>
                    </select>
                    </div>
                    <div class="form-row"><label class="form-label">أيام الأسبوع (bitmask)</label><input class="form-inp" id="r-wdays" type="number" value="127" min="0" max="127"></div>
                    <div class="form-row"><label class="form-label">تاريخ الانتهاء (فارغ للأبد)</label><input class="form-inp" id="r-exp" type="date"></div>
                    <div class="btns" style="margin-top:16px">
                    <button class="btn btn-info" onclick="saveRule()">حفظ القاعدة</button>
                    <button class="btn" onclick="closeModal('rule-modal')">إلغاء</button>
                    </div>
                    </div>
                    </div>

                    <div class="modal-bg" id="user-modal">
                    <div class="modal">
                    <div class="modal-hdr">+ مستخدم جديد</div>
                    <div class="form-row"><label class="form-label">اسم المستخدم</label><input class="form-inp" id="u-name"></div>
                    <div class="form-row"><label class="form-label">كلمة المرور</label><input class="form-inp" id="u-pass" type="password"></div>
                    <div class="form-row"><label class="form-label">الصلاحية</label>
                    <select class="form-inp" id="u-role">
                    <option value="0">Viewer</option><option value="1">Operator</option><option value="2">Admin</option>
                    </select>
                    </div>
                    <div class="btns" style="margin-top:16px">
                    <button class="btn btn-info" onclick="saveUser()">إضافة</button>
                    <button class="btn" onclick="closeModal('user-modal')">إلغاء</button>
                    </div>
                    </div>
                    </div>

                    <div class="modal-bg" id="cp-modal">
                    <div class="modal">
                    <div class="modal-hdr">🔑 تغيير كلمة المرور</div>
                    <div class="form-row"><label class="form-label">المستخدم</label><input class="form-inp" id="cp-user" value="admin"></div>
                    <div class="form-row"><label class="form-label">كلمة المرور الجديدة</label><input class="form-inp" id="cp-new" type="password"></div>
                    <div class="btns" style="margin-top:16px">
                    <button class="btn btn-info" onclick="changePass()">تغيير</button>
                    <button class="btn" onclick="closeModal('cp-modal')">إلغاء</button>
                    </div>
                    </div>
                    </div>

                    <script>
                    // ── إصلاح #3: استخدام port ديناميكي من الـ URL الحالي ──
                    var WS_PORT = )HTML";
                    html += wsPort;
                    html += R"HTML(;
var ws, pingTimer, chartInst;
var allLogs = [], lastState = {};

// ── إصلاح رئيسي: connect باستخدام الـ port الصحيح ──
function connect() {
  var wsUrl = 'ws://' + location.hostname + ':' + WS_PORT + '/';
  ws = new WebSocket(wsUrl);
  ws.onopen = function() {
    document.getElementById('ws-badge').textContent = '⬤ LIVE';
    document.getElementById('ws-badge').className = 'ws-badge ws-ok';
    pingTimer = setInterval(function(){ if(ws.readyState===1) ws.send('ping'); }, 25000);
  };
  ws.onclose = function() {
    document.getElementById('ws-badge').textContent = '⬤ DISCONNECTED';
    document.getElementById('ws-badge').className = 'ws-badge ws-err';
    clearInterval(pingTimer);
    setTimeout(connect, 3000);
  };
  ws.onerror = function() {
    document.getElementById('ws-badge').textContent = '⬤ ERROR';
    document.getElementById('ws-badge').className = 'ws-badge ws-err';
  };
  ws.onmessage = function(e) {
    try { var d = JSON.parse(e.data); if(d.relay !== undefined) updateUI(d); } catch(x) {}
  };
}

function send(cmd) { if(ws && ws.readyState===1) ws.send(cmd); }
function sendTimer() { var m = document.getElementById('t-min').value; if(m>0) send('timer:'+m); }

function fmtUptime(s) {
  var d=Math.floor(s/86400), h=Math.floor((s%86400)/3600), m=Math.floor((s%3600)/60);
  return d+'d '+('0'+h).slice(-2)+'h '+('0'+m).slice(-2)+'m';
}
function fmtRSSI(r) {
  if(r>-50) return '<span style="color:var(--green)">'+r+' dBm ▲▲▲</span>';
  if(r>-70) return '<span style="color:var(--yellow)">'+r+' dBm ▲▲</span>';
  return '<span style="color:var(--red)">'+r+' dBm ▲</span>';
}

function updateUI(d) {
  lastState = d;
  document.getElementById('k-time').textContent = d.time || '--:--';
  document.getElementById('k-date').textContent = d.date || '----';
  document.getElementById('hdr-time').textContent = d.time || '--:--';
  document.getElementById('k-today').textContent = (d.todayOnH||0).toFixed(2)+'h';
  document.getElementById('k-peak').textContent = 'ذروة: '+(d.peakOnH||0).toFixed(2)+'h';
  document.getElementById('k-kwh').textContent = (d.kWh||0).toFixed(3);
  document.getElementById('k-rssi').innerHTML = fmtRSSI(d.rssi||0);
  document.getElementById('k-ssid').textContent = d.ssid || '---';
  document.getElementById('k-heap').textContent = Math.round((d.freeHeap||0)/1024);
  document.getElementById('k-total').textContent = (d.totalOnH||0).toFixed(1)+'h';
  document.getElementById('k-cycles').textContent = (d.cycles||0)+' دورة';
  document.getElementById('k-uptime').textContent = fmtUptime(d.uptime||0);
  document.getElementById('k-fs').textContent = 'FS '+Math.round((d.fsUsed||0)/1024)+'/'+(Math.round((d.fsTotal||0)/1024))+'KB';
  var on = d.relay;
  var sv = document.getElementById('k-state');
  sv.textContent = on ? 'تشغيل' : 'إطفاء';
  sv.className = 'kpi-val ' + (on ? 'on' : 'off');
  var pp = document.getElementById('relay-pill');
  pp.textContent = '● ' + (on ? 'ACTIVE' : 'OFFLINE');
  pp.className = 'status-pill ' + (d.emergency ? 'pill-em' : (on ? 'pill-on' : 'pill-off'));
  var mode = 'AUTO';
  if(d.emergency) mode = 'EMERGENCY';
  else if(d.timer) mode = 'TIMER '+(d.timerLeft||0)+'m';
  else if(d.manual) mode = 'MANUAL';
  document.getElementById('k-mode').textContent = mode;
  document.getElementById('mode-pill').textContent = mode;
  var curFw = document.getElementById('cur-fw');
  if(curFw) curFw.textContent = d.fwVersion || '---';
  if(d.updateAvail) {
    document.getElementById('ota-badge').style.display = '';
    document.getElementById('upd-avail').style.display = '';
    var nfv = document.getElementById('new-fw-ver');
    if(nfv) nfv.textContent = d.updateVer || '---';
  }
  updateChart(d.weeklyHours || []);
  if(document.getElementById('page-system').classList.contains('active')) updateSysInfo(d);
}

function initChart() {
  var days = ['أ','ا','ث','أر','خ','ج','س'];
  var ctx = document.getElementById('weekly-chart').getContext('2d');
  chartInst = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: days,
      datasets: [{
        label: 'ساعات التشغيل',
        data: [0,0,0,0,0,0,0],
        backgroundColor: 'rgba(0,212,255,.2)',
                        borderColor: 'rgba(0,212,255,.8)',
                        borderWidth: 1,
                        borderRadius: 3
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {display:false},
        tooltip: {callbacks:{label:function(v){return v.raw.toFixed(2)+' ساعة';}}}
      },
      scales: {
        x: {grid:{color:'rgba(255,255,255,.03)'},ticks:{color:'#8899bb',font:{family:"'IBM Plex Mono'"}}},
                        y: {grid:{color:'rgba(255,255,255,.03)'},ticks:{color:'#8899bb',font:{family:"'IBM Plex Mono'"},callback:function(v){return v+'h';}},beginAtZero:true}
      }
    }
  });
}
function updateChart(data) {
  if(!chartInst || !data.length) return;
  chartInst.data.datasets[0].data = data;
  chartInst.update();
}

// ── إصلاح #3: showPage بدون event global ──
function showPage(name, el) {
  document.querySelectorAll('.page').forEach(function(p){p.classList.remove('active');});
  document.querySelectorAll('.nav-item').forEach(function(n){n.classList.remove('active');});
  document.getElementById('page-'+name).classList.add('active');
  if(el) el.classList.add('active');
  if(name==='files') refreshFiles();
  if(name==='log') renderLog();
  if(name==='editor') loadFileEditor('/config.json');
  if(name==='schedule') loadRules();
  if(name==='users') loadUsers();
  if(name==='system') updateSysInfo(lastState);
  if(name==='security') loadAuditLog();
}

function fetchLog() {
  fetch('/log.json').then(function(r){return r.json();}).then(function(data){
    allLogs = data; renderLog();
  }).catch(function(){});
}
setInterval(fetchLog, 5000);

function renderLog() {
  var search = (document.getElementById('log-search')||{}).value || '';
  search = search.toLowerCase();
  var level = (document.getElementById('log-level')||{}).value || '';
  var el = document.getElementById('main-log'); if(!el) return;
  el.innerHTML = '';
  allLogs.filter(function(e){
    if(search && !e.msg.toLowerCase().includes(search)) return false;
    if(level && !e.msg.includes(level)) return false;
    return true;
  }).forEach(function(e){
    var d = document.createElement('div');
    d.className = 'log-entry';
  d.innerHTML = '<span class="log-ts">+'+Math.floor(e.ts/1000)+'s</span><span class="log-msg">'+e.msg+'</span>';
  el.appendChild(d);
  });
  el.scrollTop = el.scrollHeight;
}
function filterLog() { renderLog(); }
function clearLog() { allLogs = []; renderLog(); }

function refreshFiles() {
  fetch('/api/fs').then(function(r){return r.json();}).then(function(d){
    document.getElementById('fs-used').textContent = Math.round(d.used/1024);
    document.getElementById('fs-total').textContent = Math.round(d.total/1024);
    var pct = Math.round(d.used/d.total*100);
    document.getElementById('fs-bar').style.width = pct+'%';
  var el = document.getElementById('file-list'); el.innerHTML = '';
  (d.files||[]).forEach(function(f){
    var r = document.createElement('div'); r.className = 'file-row';
  r.innerHTML = '<span class="file-name">'+f.name+'</span>'+
  '<span class="file-size">'+Math.round(f.size/1024*100)/100+'KB</span>'+
  '<div class="file-acts">'+
  '<button class="btn btn-sm btn-info" onclick="editFile(\''+f.name+'\')">تعديل</button>'+
  '<button class="btn btn-sm" onclick="downloadFile(\''+f.name+'\')">↓</button>'+
  '<button class="btn btn-sm btn-danger" onclick="if(confirm(\'حذف '+f.name+'؟\'))deleteFile(\''+f.name+'\')">✕</button>'+
  '</div>';
  el.appendChild(r);
  });
  }).catch(function(e){alert('خطأ: '+e);});
}
function downloadFile(name) { window.open('/api/file?path='+encodeURIComponent(name),'_blank'); }
function deleteFile(name) {
  fetch('/api/file?path='+encodeURIComponent(name),{method:'DELETE'}).then(function(){refreshFiles();}).catch(function(e){alert('خطأ: '+e);});
}
function editFile(name) {
  showPage('editor', null);
  document.getElementById('edit-file').value = name;
  loadFileEditor(name);
}
function uploadFile(inp) {
  var f = inp.files[0]; if(!f) return;
  var fd = new FormData(); fd.append('file',f); fd.append('path','/'+f.name);
  fetch('/api/upload',{method:'POST',body:fd}).then(function(){refreshFiles();alert('✅ تم الرفع');}).catch(function(e){alert('خطأ: '+e);});
}
function backupDownload() {
  fetch('/api/backup').then(function(r){return r.blob();}).then(function(b){
    var a = document.createElement('a');
    a.href = URL.createObjectURL(b);
    a.download = 'billboard_backup_'+Date.now()+'.json';
  a.click();
  });
}
function restoreBackup(inp) {
  var f = inp.files[0]; if(!f) return;
  var reader = new FileReader();
  reader.onload = function(e) {
    if(!confirm('ستُستبدل كل الإعدادات. تأكيد؟')) return;
    fetch('/api/restore',{method:'POST',headers:{'Content-Type':'application/json'},body:e.target.result})
    .then(function(r){return r.json();}).then(function(d){alert(d.ok?'✅ تم الاستعادة':'❌ فشل');}).catch(function(e){alert('خطأ: '+e);});
  };
  reader.readAsText(f);
}

function loadFileEditor(path) {
  document.getElementById('edit-name').textContent = path;
  document.getElementById('editor-status').textContent = 'جاري التحميل...';
  fetch('/api/file?path='+encodeURIComponent(path)).then(function(r){return r.text();}).then(function(t){
    document.getElementById('code-editor').value = t;
    document.getElementById('editor-status').textContent = 'محمّل ✓ — '+path;
  }).catch(function(e){document.getElementById('editor-status').textContent = 'خطأ: '+e;});
}
function saveFileEditor() {
  var path = document.getElementById('edit-name').textContent;
  var content = document.getElementById('code-editor').value;
  document.getElementById('editor-status').textContent = 'جاري الحفظ...';
  fetch('/api/file',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({path:path,content:content})})
  .then(function(r){return r.json();}).then(function(d){
    document.getElementById('editor-status').textContent = d.ok ? 'محفوظ ✓ '+new Date().toLocaleTimeString() : 'خطأ في الحفظ';
  }).catch(function(e){document.getElementById('editor-status').textContent = 'خطأ: '+e;});
}
function formatJSON() {
  try {
    var v = JSON.parse(document.getElementById('code-editor').value);
    document.getElementById('code-editor').value = JSON.stringify(v,null,2);
    document.getElementById('editor-status').textContent = 'تم التنسيق ✓';
  } catch(e) { document.getElementById('editor-status').textContent = 'JSON غير صالح: '+e.message; }
}

function setSchedMode(mode) {
  send(mode==='advanced' ? 'advsched:on' : 'advsched:off');
  document.getElementById('simple-sched').style.display = (mode==='simple' ? '' : 'none');
  if(mode==='simple' && lastState) {
    document.getElementById('sh').value = lastState.startHour || 19;
    document.getElementById('eh').value = lastState.endHour || 0;
  }
}
function saveSimpleSched() {
  var sh = document.getElementById('sh').value, eh = document.getElementById('eh').value;
  apiCmd('/cmd?action=setsched&start='+sh+'&end='+eh);
}
function loadRules() {
  fetch('/api/rules').then(function(r){return r.json();}).then(function(d){
    var el = document.getElementById('rules-list'); el.innerHTML = '';
  var typeNames = ['يومي','أسبوعي','تاريخ محدد','موسمي'];
  (d.rules||[]).forEach(function(r,i){
    var div = document.createElement('div'); div.className = 'rule-card';
  div.innerHTML = '<div class="rule-hdr">'+
  '<div class="rule-name">'+r.name+'</div>'+
  '<span class="badge badge-type">'+typeNames[r.type||0]+'</span>'+
  '<span class="badge '+(r.forceOn?'badge-on':'badge-off')+'">'+(r.forceOn?'تشغيل':'إيقاف')+'</span>'+
  '<label style="cursor:pointer"><input type="checkbox" '+(r.enabled?'checked':'')+' onchange="toggleRule('+i+',this.checked)"> مفعّل</label>'+
  '<button class="btn btn-sm btn-danger" onclick="deleteRule('+i+')">✕</button>'+
  '</div>'+
  '<div class="rule-time">'+('0'+r.startH).slice(-2)+':'+('0'+r.startM).slice(-2)+' → '+('0'+r.endH).slice(-2)+':'+('0'+r.endM).slice(-2)+'</div>'+
  '<div style="font-size:.75em;color:var(--text2);font-family:var(--mono);margin-top:4px">تشغيلات: '+r.triggers+'</div>';
  el.appendChild(div);
  });
  if(!(d.rules||[]).length) el.innerHTML = '<div style="color:var(--text2);font-family:var(--mono);font-size:.82em">لا توجد قواعد — أضف قاعدة جديدة</div>';
  }).catch(function(){
    document.getElementById('rules-list').innerHTML = '<div style="color:var(--text2);font-family:var(--mono);font-size:.82em">الجدول المتقدم غير مفعّل</div>';
  });
}
function openRuleModal() { document.getElementById('rule-modal').classList.add('open'); }
function closeModal(id) { document.getElementById(id).classList.remove('open'); }
function saveRule() {
  var rule = {
    name: document.getElementById('r-name').value,
    type: +document.getElementById('r-type').value,
    priority: +document.getElementById('r-pri').value,
    enabled: true,
    forceOn: +document.getElementById('r-force').value===1,
      startH: +document.getElementById('r-sh').value,
      startM: +document.getElementById('r-sm').value,
      endH: +document.getElementById('r-eh').value,
      endM: +document.getElementById('r-em').value,
      weekDays: +document.getElementById('r-wdays').value,
      triggers: 0
  };
  fetch('/api/rules',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(rule)})
  .then(function(){closeModal('rule-modal');loadRules();}).catch(function(e){alert('خطأ: '+e);});
}
function toggleRule(i,en) {
  fetch('/api/rules/'+i,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({enabled:en})}).then(function(){loadRules();});
}
function deleteRule(i) {
  if(confirm('حذف القاعدة؟')) fetch('/api/rules/'+i,{method:'DELETE'}).then(function(){loadRules();});
}

var roleNames = ['Viewer','Operator','Admin'];
var roleCss = ['role-view','role-op','role-admin'];
function loadUsers() {
  fetch('/api/users').then(function(r){return r.json();}).then(function(d){
    var el = document.getElementById('users-list'); el.innerHTML = '';
  (d.users||[]).forEach(function(u,i){
    var r = document.createElement('div'); r.className = 'user-row';
  r.innerHTML = '<div class="user-name">'+u.username+'</div>'+
  '<span class="role-badge '+roleCss[u.role]+'">'+roleNames[u.role]+'</span>'+
  '<span style="color:'+(u.enabled?'var(--green)':'var(--red)')+'">'+(u.enabled?'● فعّال':'● موقوف')+'</span>'+
  '<div class="file-acts">'+
  '<button class="btn btn-sm" onclick="toggleUser('+i+','+u.enabled+')">'+(u.enabled?'إيقاف':'تفعيل')+'</button>'+
  '<button class="btn btn-sm btn-danger" onclick="deleteUser('+i+')">✕</button>'+
  '</div>';
  el.appendChild(r);
  });
  }).catch(function(){});
}
function openUserModal() { document.getElementById('user-modal').classList.add('open'); }
function saveUser() {
  var u = {
    username: document.getElementById('u-name').value,
    password: document.getElementById('u-pass').value,
    role: +document.getElementById('u-role').value
  };
  fetch('/api/users',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(u)})
  .then(function(){closeModal('user-modal');loadUsers();}).catch(function(e){alert('خطأ: '+e);});
}
function deleteUser(i) {
  if(confirm('حذف المستخدم؟')) fetch('/api/users/'+i,{method:'DELETE'}).then(function(){loadUsers();});
}
function toggleUser(i,cur) {
  fetch('/api/users/'+i,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({enabled:!cur})}).then(function(){loadUsers();});
}

function loadAuditLog() {
  fetch('/api/audit').then(function(r){return r.json();}).then(function(d){
    var el = document.getElementById('audit-log-wrap'); el.innerHTML = '';
  (d.entries||[]).slice(-50).reverse().forEach(function(e){
    var div = document.createElement('div'); div.className = 'log-entry';
  div.innerHTML = '<span class="log-ts" style="min-width:80px">+'+Math.floor(e.ts/1000)+'s</span>'+
  '<span class="log-msg" style="color:'+(e.ok?'var(--green)':'var(--red)')+'">'+e.user+' → '+e.action+'</span>'+
  '<span class="log-ts">'+e.ip+'</span>';
  el.appendChild(div);
  });
  }).catch(function(){});
}
function revokeAllTokens() {
  fetch('/api/revoke',{method:'POST'}).then(function(){alert('✅ تم إلغاء كل الجلسات');});
}
function saveWhitelist() {
  var ips = document.getElementById('ip-whitelist').value.split('\n').filter(function(l){return l.trim();});
  fetch('/api/whitelist',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ips:ips})})
  .then(function(r){return r.json();}).then(function(d){alert(d.ok?'✅ Whitelist محفوظ':'❌ خطأ');});
}
function openChangePassModal() { document.getElementById('cp-modal').classList.add('open'); }
function changePass() {
  var u = document.getElementById('cp-user').value, p = document.getElementById('cp-new').value;
  fetch('/api/users/password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:u,password:p})})
  .then(function(r){return r.json();}).then(function(d){if(d.ok){closeModal('cp-modal');alert('✅ تم');}else alert('❌ فشل');});
}

function handleDrop(e) { e.preventDefault(); e.currentTarget.classList.remove('drag'); uploadFirmware(e.dataTransfer.files[0]); }
function uploadFirmware(f) {
  if(!f||!f.name.endsWith('.bin')) { alert('اختر ملف .bin صالح'); return; }
  if(!confirm('رفع '+f.name+' ('+Math.round(f.size/1024)+'KB)؟')) return;
  var pb=document.getElementById('fw-progress'), fill=document.getElementById('fw-fill'), st=document.getElementById('fw-status');
  pb.style.display=''; st.style.display=''; st.textContent='جاري الرفع...';
  var xhr = new XMLHttpRequest();
  xhr.upload.onprogress = function(e) {
    if(e.lengthComputable) { var p=Math.round(e.loaded/e.total*100); fill.style.width=p+'%'; st.textContent='رفع: '+p+'%'; }
  };
  xhr.onload = function() {
    if(xhr.status===200) { st.textContent='✅ تم الرفع — جاري إعادة التشغيل...'; fill.style.background='var(--green)'; setTimeout(function(){location.reload();},5000); }
    else { st.textContent='❌ فشل: '+xhr.status; fill.style.background='var(--red)'; }
  };
  xhr.onerror = function() { st.textContent='❌ خطأ في الشبكة'; };
  xhr.open('POST','/api/ota');
  xhr.setRequestHeader('Content-Type','application/octet-stream');
  xhr.setRequestHeader('X-Filename',f.name);
  xhr.send(f);
}
function checkGitHub() {
  fetch('/api/checkupdate').then(function(r){return r.json();}).then(function(d){
    if(d.available) alert('⬆ إصدار جديد: '+d.version);
    else alert('✅ الإصدار الحالي محدّث');
  });
}
function performSelfUpdate() {
  if(!lastState.updateAvail) { alert('لا يوجد تحديث متاح'); return; }
  if(confirm('تحديث من '+lastState.updateVer+'؟')) apiCmd('/api/selfupdate');
}

function updateSysInfo(d) {
  var el = document.getElementById('sys-info'); if(!el) return;
  var rows = [
    ['Firmware', d.fwVersion||'---'], ['IP', d.ip||'---'], ['SSID', d.ssid||'---'],
    ['RSSI', (d.rssi||0)+' dBm'], ['Free Heap', Math.round((d.freeHeap||0)/1024)+' KB'],
    ['Uptime', fmtUptime(d.uptime||0)], ['Messages', d.msgCount||0],
    ['FS Used', Math.round((d.fsUsed||0)/1024)+' / '+Math.round((d.fsTotal||0)/1024)+' KB'],
    ['Total kWh', (d.kWh||0).toFixed(3)], ['Power Cycles', d.cycles||0],
    ['Load Watts', (d.loadW||0)+' W'],
    ['Update', d.updateAvail?'متاح '+d.updateVer:'لا يوجد'],
  ];
  el.innerHTML = rows.map(function(row){
    return '<span style="color:var(--accent2)">'+row[0]+':</span> <span style="color:var(--text)">'+row[1]+'</span><br>';
  }).join('');
}

function saveMQTT() {
  var data = {
    server: document.getElementById('mqtt-srv').value,
    port: +document.getElementById('mqtt-port').value,
    user: document.getElementById('mqtt-user').value,
    pass: document.getElementById('mqtt-pass').value
  };
  fetch('/api/mqtt',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)})
  .then(function(r){return r.json();}).then(function(d){alert(d.ok?'✅ MQTT محفوظ':'❌ خطأ');});
}

function apiCmd(url, opts) {
  fetch(url, opts||{}).then(function(r){return r.json();}).then(function(d){
    if(d.msg) console.log(d.msg);
  }).catch(function(e){console.error(e);});
}

// بدء التشغيل
document.addEventListener('DOMContentLoaded', function() {
  initChart();
  connect();
  fetchLog();
  // جلب الحالة الأولية عبر HTTP (fallback لحين اتصال WS)
  fetch('/status').then(function(r){return r.json();}).then(function(d){updateUI(d);}).catch(function(){});
});
</script>
</body></html>
)HTML";

  return html;
}

void registerAPIRoutes() {
  webServer.on("/", HTTP_GET, serveDashboard);
  webServer.on("/status", HTTP_GET, [](){
    webServer.send(200,"application/json", buildStatusJson());
  });
  webServer.on("/log.json", HTTP_GET, [](){
    String json="[";
    int cnt=min(logIdx,LOG_SIZE);
    for(int j=cnt-1;j>=0;j--){
      int idx=(logIdx-1-j+LOG_SIZE)%LOG_SIZE;
      String msg = eventLog[idx%LOG_SIZE].msg;
      msg.replace("\"","\\\"");
      json+="{\"ts\":"+String(eventLog[idx%LOG_SIZE].ts)+",\"msg\":\""+msg+"\"}";
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
    else if(a=="reboot"){
      webServer.send(200,"application/json","{\"ok\":true}");
      delay(1000);saveConfigJSON();ESP.restart();return;
    }
    else if(a=="resetstats"){stats.totalOnSec=0;stats.todayOnSec=0;stats.powerCycles=0;stats.estimatedKWh=0;saveConfigJSON();}
    else if(a=="saveconfig"){saveConfigJSON();saveScheduleJSON();saveUsersJSON();}
    else if(a=="setsched"){
      sys.startHour=webServer.arg("start").toInt();
      sys.endHour=webServer.arg("end").toInt();
      saveConfigJSON();updateRelay();
    }
    audit("api","cmd:"+a,ip);
    webServer.send(200,"application/json","{\"ok\":true}");
  });

  #ifdef USE_LITTLEFS
  webServer.on("/api/fs", HTTP_GET, [](){ webServer.send(200,"application/json",getFSInfo()); });

  webServer.on("/api/file", HTTP_GET, [](){
    String path=webServer.arg("path");
    if(path.length()==0||!FS_IMPL.exists(path)){webServer.send(404,"text/plain","Not Found");return;}
    File f=FS_IMPL.open(path,"r");
    webServer.streamFile(f,"text/plain; charset=utf-8");
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

  webServer.on("/api/upload", HTTP_POST, [](){webServer.send(200,"application/json","{\"ok\":true}");},[](){
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
  #endif // USE_LITTLEFS

  // ── OTA عبر API ──
  webServer.on("/api/ota", HTTP_POST, [](){webServer.send(200,"application/json","{\"ok\":true}");},[](){
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

  // ── Users API ──
  webServer.on("/api/users", HTTP_GET, [](){
    DynamicJsonDocument doc(4096);
    JsonArray arr=doc.createNestedArray("users");
    for(auto&u:users){
      JsonObject o=arr.createNestedObject();
      o["username"]=u.username;o["role"]=(int)u.role;o["enabled"]=u.enabled;
    }
    String out;serializeJson(doc,out);webServer.send(200,"application/json",out);
  });

  webServer.on("/api/users", HTTP_POST, [](){
    DynamicJsonDocument doc(1024);
    if(deserializeJson(doc,webServer.arg("plain"))){webServer.send(400,"application/json","{\"ok\":false}");return;}
    String uname=doc["username"].as<String>();
    String pass=doc["password"].as<String>();
    int role=doc["role"]|0;
    users.push_back({uname,md5Hash(pass+"billboard"),(UserRole)role,true});
    saveUsersJSON();
    audit("api","user_add:"+uname,webServer.client().remoteIP().toString());
    webServer.send(200,"application/json","{\"ok\":true}");
  });

  webServer.on("/api/users/password", HTTP_POST, [](){
    DynamicJsonDocument doc(512);
    if(deserializeJson(doc,webServer.arg("plain"))){webServer.send(400,"application/json","{\"ok\":false}");return;}
    String uname=doc["username"].as<String>();
    String pass=doc["password"].as<String>();
    bool found=false;
    for(auto&u:users){if(u.username==uname){u.passHash=md5Hash(pass+"billboard");found=true;break;}}
    if(found){saveUsersJSON();audit("api","passwd:"+uname,webServer.client().remoteIP().toString());webServer.send(200,"application/json","{\"ok\":true}");}
    else webServer.send(404,"application/json","{\"ok\":false}");
  });

  webServer.on("/api/login", HTTP_POST, [](){
    DynamicJsonDocument doc(512);
    if(deserializeJson(doc,webServer.arg("plain"))){webServer.send(400,"application/json","{\"error\":\"invalid json\"}");return;}
    String uname=doc["username"].as<String>();
    String pass=doc["password"].as<String>();
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

  webServer.on("/api/revoke", HTTP_POST, [](){
    tokens.clear();
    audit("api","revoke_all",webServer.client().remoteIP().toString());
    webServer.send(200,"application/json","{\"ok\":true}");
  });

  webServer.on("/api/audit", HTTP_GET, [](){
    DynamicJsonDocument doc(8192);
    JsonArray arr=doc.createNestedArray("entries");
    int cnt=min(auditIdx,AUDIT_SIZE);
    for(int i=cnt-1;i>=0;i--){
      int idx=(auditIdx-1-i+AUDIT_SIZE)%AUDIT_SIZE;
      JsonObject o=arr.createNestedObject();
      o["ts"]=auditLog[idx%AUDIT_SIZE].ts;
      o["user"]=auditLog[idx%AUDIT_SIZE].user;
      o["action"]=auditLog[idx%AUDIT_SIZE].action;
      o["ip"]=auditLog[idx%AUDIT_SIZE].ip;
      o["ok"]=auditLog[idx%AUDIT_SIZE].success;
    }
    String out;serializeJson(doc,out);webServer.send(200,"application/json",out);
  });

  webServer.on("/api/whitelist", HTTP_POST, [](){
    DynamicJsonDocument doc(2048);
    if(deserializeJson(doc,webServer.arg("plain"))){webServer.send(400,"application/json","{\"ok\":false}");return;}
    ipWhitelist.clear();
    for(String ip:doc["ips"].as<JsonArray>()) if(ip.length()>0) ipWhitelist.push_back(ip);
    saveUsersJSON();
    webServer.send(200,"application/json","{\"ok\":true}");
  });

  webServer.on("/api/mqtt", HTTP_POST, [](){
    DynamicJsonDocument doc(1024);
    if(deserializeJson(doc,webServer.arg("plain"))){webServer.send(400,"application/json","{\"ok\":false}");return;}
    mqttServer=doc["server"].as<String>();
    mqttPort=doc["port"]|1883;
    mqttUser=doc["user"].as<String>();
    mqttPassStr=doc["pass"].as<String>();
    mqtt.disconnect();
    mqtt.setServer(mqttServer.c_str(),mqttPort);
    saveConfigJSON();
    webServer.send(200,"application/json","{\"ok\":true}");
  });

  webServer.on("/api/checkupdate", HTTP_GET, [](){
    #ifdef USE_SELF_UPDATE
    checkGitHubUpdate();
    webServer.send(200,"application/json","{\"available\":"+String(sys.updateAvailable?"true":"false")+",\"version\":\""+sys.updateVersion+"\"}");
    #else
    webServer.send(200,"application/json","{\"available\":false,\"version\":\"\"}");
    #endif
  });

  webServer.onNotFound([](){
    String url = webServer.uri();

    #ifdef USE_ADVANCED_SCHEDULE
    if(url.startsWith("/api/rules")) {
      if(url == "/api/rules" || url == "/api/rules/") {
        if(webServer.method()==HTTP_GET) {
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
          String out;serializeJson(doc,out);
          webServer.send(200,"application/json",out);
          return;
        }
        if(webServer.method()==HTTP_POST) {
          DynamicJsonDocument doc(2048);
          if(deserializeJson(doc,webServer.arg("plain"))){webServer.send(400,"application/json","{\"ok\":false}");return;}
          ScheduleRule r;
          r.name=doc["name"].as<String>();r.type=doc["type"]|0;r.priority=doc["priority"]|1;
          r.enabled=doc["enabled"]|true;r.forceOn=doc["forceOn"]|true;
          r.startH=doc["startH"]|19;r.startM=doc["startM"]|0;
          r.endH=doc["endH"]|0;r.endM=doc["endM"]|0;
          r.weekDays=doc["weekDays"]|0x7F;r.month=0;r.day=0;r.seasonStart=0;r.seasonEnd=0;
          r.expiryDate=(time_t)(long long)(doc["expiry"]|0);r.triggerCount=0;
          scheduleRules.push_back(r);saveScheduleJSON();
          audit("api","rule_add:"+r.name,webServer.client().remoteIP().toString());
          webServer.send(200,"application/json","{\"ok\":true}");
          return;
        }
      }

      if(url.length() > 11) {
        int idx = url.substring(11).toInt();  // بعد "/api/rules/"
        if(idx < 0 || idx >= (int)scheduleRules.size()){
          webServer.send(404,"application/json","{\"error\":\"not found\"}"); return;
        }
        if(webServer.method()==HTTP_DELETE){
          scheduleRules.erase(scheduleRules.begin()+idx);
          saveScheduleJSON();
          webServer.send(200,"application/json","{\"ok\":true}");
          return;
        }
        if(webServer.method()==HTTP_PATCH){
          DynamicJsonDocument doc(512);
          deserializeJson(doc,webServer.arg("plain"));
          if(doc.containsKey("enabled")) scheduleRules[idx].enabled=(bool)doc["enabled"];
          if(doc.containsKey("forceOn")) scheduleRules[idx].forceOn=(bool)doc["forceOn"];
          saveScheduleJSON();
          webServer.send(200,"application/json","{\"ok\":true}");
          return;
        }
      }
    }
    #endif // USE_ADVANCED_SCHEDULE
    if(url.startsWith("/api/users/") && url.length() > 11) {
      String sub = url.substring(11);
      if(sub != "password") {
        int idx = sub.toInt();
        if(idx >= 0 && idx < (int)users.size()) {
          if(webServer.method()==HTTP_DELETE){
            users.erase(users.begin()+idx);
            saveUsersJSON();
            webServer.send(200,"application/json","{\"ok\":true}");
            return;
          }
          if(webServer.method()==HTTP_PATCH){
            DynamicJsonDocument doc(512);
            deserializeJson(doc,webServer.arg("plain"));
            if(doc.containsKey("enabled")) users[idx].enabled=(bool)doc["enabled"];
            if(doc.containsKey("role")) users[idx].role=(UserRole)(int)doc["role"];
            saveUsersJSON();
            webServer.send(200,"application/json","{\"ok\":true}");
            return;
          }
        }
      }
    }

    webServer.send(404,"text/plain","Not Found");
  });

  ElegantOTA.begin(&webServer,Config::OTA_USER,Config::OTA_PASS);
  ElegantOTA.onStart([](){sys.otaInProgress=true;addLog("OTA: بدأ");});
  ElegantOTA.onEnd([](bool ok){
    sys.otaInProgress=!ok;
    addLog(ok?"OTA: ✓":"OTA: ✗");
    bot.sendMessage(Config::ALLOWED_CHATS[0],ok?"✅ تحديث ناجح":"❌ تحديث فشل");
  });

  webServer.begin();
  addLog("Web API: :"+String(Config::WEB_PORT));
}

void handleMessages(int count){
  for(int i=0;i<count;i++){
    String chatId=bot.messages[i].chat_id;
    String text=bot.messages[i].text;
    String fname=bot.messages[i].from_name;
    text.trim();sys.msgCount++;
    if(!isAllowedChat(chatId)){bot.sendMessage(chatId,"⛔ غير مصرح");continue;}
    if(adminPassword.length()>0&&!isChatAuth(chatId)){
      if(text.startsWith("/login ")){
        String pass=text.substring(7); pass.trim();
        if(pass==adminPassword){
          authenticatedChats.push_back(chatId);
          audit("telegram","login:"+chatId,fname);
          bot.sendMessage(chatId,"✅ مرحباً "+fname+"!");
        } else {
          audit("telegram","login_fail:"+chatId,fname,false);
          bot.sendMessage(chatId,"❌ خاطئة");
        }
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
    else if(text.startsWith("/setstart ")){
      int h=text.substring(10).toInt();
      if(h>=0&&h<24){sys.startHour=h;saveConfigJSON();updateRelay();bot.sendMessage(chatId,"✅ بدء: "+String(h)+":00");}
    }
    else if(text.startsWith("/setend ")){
      int h=text.substring(8).toInt();
      if(h>=0&&h<=24){sys.endHour=h==24?0:h;saveConfigJSON();updateRelay();bot.sendMessage(chatId,"✅ نهاية: "+String(h)+":00");}
    }
    else if(text.startsWith("/timezone ")){
      int z=text.substring(10).toInt();
      if(z>=-12&&z<=14){sys.tzOffset=z*3600;configTime(sys.tzOffset,0,"pool.ntp.org");saveConfigJSON();bot.sendMessage(chatId,"✅ UTC+"+String(z));}
    }
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
      bot.sendMessage(chatId,"💾 جاري الإنشاء...");
      String bk=buildFullBackup();
      bot.sendMessage(chatId,"```\n"+bk.substring(0,min(3000,(int)bk.length()))+"\n```","Markdown");
    }
    #ifdef USE_SELF_UPDATE
    else if(text=="/checkupdate"){checkGitHubUpdate();}
    else if(text=="/selfupdate"){
      if(sys.updateAvailable)bot.sendMessage(chatId,"⬆ تحديث جاري...");
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
      m+="BLE: "+String(bleConnected?"✅ متصل":"📢 يعلن")+"\n";
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
      adminPassword=text.substring(13); adminPassword.trim();
      saveConfigJSON();
      bot.sendMessage(chatId,adminPassword.length()==0?"✅ محذوفة":"✅ محدّثة");
    }
    else if(text=="/logout"){
      authenticatedChats.erase(std::remove(authenticatedChats.begin(),authenticatedChats.end(),chatId),authenticatedChats.end());
      bot.sendMessage(chatId,"👋");
    }
    else if(text=="/nextpage"){sys.displayPage++;updateDisplay();bot.sendMessage(chatId,"🖥️ صفحة "+String(sys.displayPage%5));}
    else if(text.startsWith("/page ")){sys.displayPage=text.substring(6).toInt();updateDisplay();}
    else if(text.startsWith("/setwatts ")){
      float w=text.substring(10).toFloat();
      if(w>0&&w<=10000){LOAD_WATTS=w;saveConfigJSON();bot.sendMessage(chatId,"✅ "+String(w,0)+" واط");}
    }
    else if(text=="/help"||text=="/start"){
      bot.sendMessage(chatId,
                      "🤖 *Smart Billboard v6.0 ULTIMATE*\n━━━━━━━━━━━\n"
                      "⚡ /on /off /auto /toggle\n🚨 /emergency ⏱️ /timer N\n🔒 /lock /unlock\n"
                      "━━━━━━━━━━━\n"
                      "📊 /stats /setwatts /resetstats\n"
                      "⏰ /setstart /setend /timezone\n🖥️ /nextpage /page N\n"
                      "━━━━━━━━━━━\n"
                      "📡 /network\n"
                      "💾 /backup\n"
                      "🔄 /checkupdate /selfupdate\n"
                      "🔐 /setpassword /login /logout\n"
                      "🔧 /ip /ota /log /status /reboot","Markdown");
    }
    else if(text.startsWith("/")) bot.sendMessage(chatId,"❓ /help");

    updateDisplay();
    #ifdef USE_WEBSOCKET
    wsBroadcast();
    #endif
  }
}

void setup() {
  Serial.begin(115200);
  Serial.println(F("\n╔══════════════════════════════════════╗"));
  Serial.println(F("║  Smart Billboard v6.0 ULTIMATE FIXED ║"));
  Serial.println(F("╚══════════════════════════════════════╝\n"));

  pinMode(Config::RELAY_PIN, OUTPUT); digitalWrite(Config::RELAY_PIN, LOW);
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
  #else
  prefs.begin("billboard",true);
  sys.startHour  = prefs.getInt("startHour",Config::DEFAULT_START);
  sys.endHour    = prefs.getInt("endHour",Config::DEFAULT_END);
  sys.tzOffset   = prefs.getInt("tzOffset",Config::DEFAULT_TZ*3600);
  adminPassword  = prefs.getString("adminPass","");
  stats.totalOnSec  = prefs.getULong("totOn",0);
  stats.powerCycles = prefs.getInt("cycles",0);
  stats.estimatedKWh= prefs.getFloat("kWh",0.0);
  prefs.end();
  #endif

  sys.bootTime=millis(); sys.fwVersion="v6.0-ULTIMATE";
  sys.manualOverride=sys.relayOn=sys.otaInProgress=sys.emergencyMode=sys.controlLocked=false;
  sys.captivePortalActive=sys.timerActive=sys.updateAvailable=false;
  sys.displayPage=0; sys.msgCount=0; sys.connectedNetIdx=-1;
  sys.peakOnHours=0; sys.lastGhCheck=""; sys.updateVersion="";
  sys.useWeekSchedule=false; sys.useAdvancedSchedule=false;
  sys.lastWsPush=0; sys.lastStatsSave=0; sys.lastMqttPub=0;
  sys.lastBotCheck=0; sys.lastRelayCheck=0; sys.lastDisplayUpd=0; sys.lastSensorRead=0;

  memset(&stats, 0, sizeof(stats));
  stats.lastLogDay=-1;

  WiFi.setHostname("SmartBillboard");
  WiFi.mode(WIFI_STA);
  for(int n=0;n<3&&sys.connectedNetIdx<0;n++){
    if(!WIFI_NETS[n].ssid||strlen(WIFI_NETS[n].ssid)==0) continue;
    addLog("WiFi: محاولة "+String(WIFI_NETS[n].ssid));
    WiFi.begin(WIFI_NETS[n].ssid,WIFI_NETS[n].pass);
    for(int a=0;a<20&&WiFi.status()!=WL_CONNECTED;a++) delay(500);
    if(WiFi.status()==WL_CONNECTED){
      sys.wifiConnected=true; sys.connectedNetIdx=n;
      addLog("WiFi: ✓ "+String(WIFI_NETS[n].ssid)+" "+WiFi.localIP().toString());
    } else {
      WiFi.disconnect(); delay(200);
    }
  }

  if(!sys.wifiConnected){
    errorScreen("WiFi Failed","AP Mode...");
    #ifdef USE_CAPTIVE_PORTAL
    startCaptivePortal(); return;
    #endif
  }

  // NTP
  configTime(sys.tzOffset,0,"pool.ntp.org","time.nist.gov");
  for(int i=0;i<15;i++){
    struct tm t;
    if(getLocalTime(&t)){addLog("NTP: "+getTimeStr());break;}
    delay(400);
  }

  secureClient.setInsecure();

  #ifdef USE_MDNS
  if(MDNS.begin(Config::MDNS_NAME)){
    MDNS.addService("http","tcp",Config::WEB_PORT);
    addLog("mDNS: "+String(Config::MDNS_NAME)+".local");
  }
  #endif

  registerAPIRoutes();

  #ifdef USE_WEBSOCKET
  wsServer.begin();
  wsServer.onEvent(wsEvent);
  addLog("WebSocket: :81 جاهز");
  #endif

  #ifdef USE_BLE
  initBLE();
  #endif

  // MQTT
  if(mqttServer.length()>0){
    mqtt.setServer(mqttServer.c_str(),mqttPort);
    mqtt.setCallback(mqttCallback);
    mqttReconnect();
  }

  #ifdef USE_ESPNOW
  initESPNow();
  #endif

  #ifdef USE_DHT_SENSOR
  dht.begin();
  #endif

  // Watchdog
  esp_task_wdt_config_t wdt={.timeout_ms=(uint32_t)Config::WDT_S*1000,.idle_core_mask=0,.trigger_panic=true};
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
  welcome+="📡 WS: `ws://"+WiFi.localIP().toString()+":81`\n";
  welcome+="━━━━━━━━━━━\n✨ *الإصلاحات:*\n• WebSocket مفعّل ✓\n• Dashboard يعمل ✓\n• onNotFound موحّد ✓\n/help للقائمة";
  bot.sendMessage(Config::ALLOWED_CHATS[0],welcome,"Markdown");
  audit("system","startup",WiFi.localIP().toString());

  checkDailyReset();
  updateRelay();
  updateDisplay();

  Serial.println(F("\n═══ v6.0 ULTIMATE FIXED — Ready ═══\n"));
}

void loop(){
  esp_task_wdt_reset();

  #ifdef USE_CAPTIVE_PORTAL
  if(sys.captivePortalActive){handleCaptivePortal();SmartButton::update();return;}
  #endif

  if(WiFi.status()!=WL_CONNECTED){
    sys.wifiConnected=false;
    for(int n=0;n<3;n++){
      if(!WIFI_NETS[n].ssid||strlen(WIFI_NETS[n].ssid)==0) continue;
      WiFi.begin(WIFI_NETS[n].ssid,WIFI_NETS[n].pass);
      for(int a=0;a<10&&WiFi.status()!=WL_CONNECTED;a++) delay(500);
      if(WiFi.status()==WL_CONNECTED){
        sys.wifiConnected=true; sys.connectedNetIdx=n;
        addLog("إعادة اتصال: "+String(WIFI_NETS[n].ssid));
        if(mqttServer.length()>0) mqttReconnect();
        break;
      }
      WiFi.disconnect(); delay(200);
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
  if(now-sys.lastWsPush>=(unsigned long)Config::WS_PUSH_MS){
    sys.lastWsPush=now;
    wsBroadcast();
  }
  #endif

  #ifdef USE_BLE
  if(bleConnected) updateBLEStatus();
  #endif

  // MQTT
  if(mqttServer.length()>0){
    if(!mqtt.connected()) mqttReconnect();
    mqtt.loop();
    if(now-sys.lastMqttPub>=60000){
      sys.lastMqttPub=now;
      if(mqtt.connected()) mqtt.publish(mqttTopicState.c_str(),buildStatusJson().c_str(),true);
    }
  }

  SmartButton::update();

  // Telegram
  if(now-sys.lastBotCheck>=(unsigned long)Config::BOT_POLL_MS){
    sys.lastBotCheck=now;
    int n=bot.getUpdates(bot.last_message_received+1);
    while(n){handleMessages(n);n=bot.getUpdates(bot.last_message_received+1);}
  }

  if(now-sys.lastRelayCheck>=(unsigned long)Config::RELAY_CHK_S*1000){
    sys.lastRelayCheck=now;
    updateRelay();
    checkDailyReset();
  }

  #ifdef USE_DHT_SENSOR
  if(now-sys.lastSensorRead>=60000){
    sys.lastSensorRead=now;
    float h=dht.readHumidity(), tc=dht.readTemperature();
    if(!isnan(h)&&!isnan(tc)){
      humidity=h; temperature=tc; sensorOK=true;
      if(tc>=Config::TEMP_ALERT_H&&!tempAlertSent){
        bot.sendMessage(Config::ALLOWED_CHATS[0],"🌡️ *حرارة "+String(tc,1)+"°C*","Markdown");
        tempAlertSent=true;
      } else if(tc<Config::TEMP_ALERT_H-3) tempAlertSent=false;
    } else sensorOK=false;
  }
  #endif

  if(now-sys.lastStatsSave>=(unsigned long)Config::STATS_SAVE_S*1000){
    sys.lastStatsSave=now;
    if(sys.relayOn&&stats.lastOnTime>0){
      unsigned long ex=(now-stats.lastOnTime)/1000;
      stats.totalOnSec+=ex; stats.todayOnSec+=ex;
      stats.estimatedKWh+=(LOAD_WATTS*ex)/3600000.0;
      if(stats.todayOnSec/3600.0>sys.peakOnHours) sys.peakOnHours=stats.todayOnSec/3600.0;
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
    if(!sys.controlLocked&&!sys.emergencyMode) sys.displayPage++;
    updateDisplay();
  }

  tokens.erase(std::remove_if(tokens.begin(),tokens.end(),
                              [&](const AuthToken&t){return millis()>t.expiry;}),tokens.end());
}
