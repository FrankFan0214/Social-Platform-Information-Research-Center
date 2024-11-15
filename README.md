## 專案背景
隨著網路普及，社群媒體已成為現代人交流、分享資訊的主要平台。
無數的貼文、留言和分享匯聚而成的大數據資料，承載著大眾對於各種議題的看法、情緒及關注焦點。
這些資訊若能適時分析，將能為商業決策提供及時且具體的參考。
我們的專案旨在利用社群媒體數據分析，探測當前的輿論趨勢及情緒變化，幫助企業在最適當的時機投入網路行銷活動。

## 專案目標
1. 議題偵測：藉由持續分析社群媒體中的熱門話題和關鍵字，掌握大眾的關注議題。
2. 情緒分析：分析網友對於特定議題的情緒趨勢，判斷議題為正向、負向或中立。
3. 商業應用建議：供商業訊息投放根據輿論趨勢及情緒波動，判斷最佳行銷時機，以達成精準的市場投入。

## 流程架構與工具運用
![Blank diagram](https://github.com/user-attachments/assets/0df26931-026a-4b64-af24-61c063e6bd03)
分別在PTT、Dcard、News進行文章資訊的爬蟲，將蒐集到的資料統一produce到Kafka。
再從Kafka consume下來後，透過CKIP Transformers對文章的內容以及留言進行分詞分析，將分析後的資料存至MongoDB資料庫。
從MongoDB取出分詞後的資料，經過Data Pipeline，轉換成BI所需要的資料表並存放在MySQL資料庫。
最後透過Tableau進行資料視覺化，產生出分析的報表。

## 關鍵程式與技術
### Crawler
1. PTT Crawler (Producer)
>在記憶體中執行網站瀏覽，以討論版逐頁輪序讀取的方式抓取文章相關訊息，可透過時間限制最後抓取日期或以網頁頁數決定爬蟲範圍，將爬取結果上傳至Kafka。
2. Dcard Crawler (Producer)
>不同於PTT，Dcard需要模擬使用者行為，因此透過打開瀏覽器方式，讀取HTML內容訊息以截取文章相關訊息，文章主體、留言、心情符號，個別抓取後統整輸出至Kafka。
3. ETtoday  Crawler (Producer)
>新聞網利用網站提供的每篇新聞連結，來擷取頁面所有訊息，範圍可設定當日或指定日期的所有新聞，並依照擷取文字內容的類型分別儲存，再上傳至Kafka。

### Kafka 部署應用
* 議題需求：
>為了面向指定單位有各自輿情觀測需求，需要將原始資料透過各自的輿情分析模型產出報告，故須提供各單位方便取用的數據渠道。
* 解決方案：
>透過建立kafka，使各單位訂閱其需要的Topic，各自取用，不會互相影響，開發producer與consumer，嵌入特定商業邏輯（爬蟲、資料導入、NLP）。
* 部署環境：​
    * GCP虛擬機​
    * 創建topic:ptt-topic、dcard-topic、news-topic
* 功能實現​
    * Crawl Producer：Ptt、Dcard、News
    * Ckip consumer：News 、Ptt、Dcard

### Airflow 部署應用
* 議題需求：
>輿情分析具有即時性與時效性，需要能夠隨時獲取最新的輿論狀況，並且產出相應的分析報表。
* 解決方案：
    * 最小方案：使用docker快速部署Airflow local executor，提供單一節點處理所有DAG。
    * 最佳配置：使用docker compose部署Airflow Master與Worker，提供多節點處理所有DAG。
* 部署環境 :
    * GCP虛擬機
    * 一台Master主節點、與多台worker節點
* 功能實現​
    * 為了能夠確保所有節點都能夠取用相同的DAG目錄，需要安裝並正確配置了 GlusterFS 客戶端​
    * Master：透過docker compose，將Airflow、MySQL、Redis各項服務安裝於目標虛擬機​
    * Worker：透過docker compose，將Airflow與tasks執行python環境與套件安裝於目標虛擬機

### NLP 文本分析
* 議題需求：​
>本案為輿情分析，需要對文本進行分詞、貼標與辨識，並且需要判斷文本的情緒，以滿足輿情觀察的目的
* 部署環境：​GCP虛擬機
* 功能實現​
  * CKIP Transformer​
  * 情緒分析模型

### CKIP Transformer​

### 情緒詞分析​

### GCP
將Kafka、MongoDB、MySQL、Airflow等系統運用雲端系統管理服務，無時無刻都可以存取並監控運行狀態。
