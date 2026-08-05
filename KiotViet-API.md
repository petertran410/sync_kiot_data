| CÔNG | TY CỔ PHẦN | CÔNG NGHỆ | KIOTVIET |     |
| ---- | ---------- | --------- | -------- | --- |
Hotline:02439904991
Trụsởchính:Tầng6-7,số1BYếtKiêu,PhườngTrầnHưngĐạo,QuậnHoànKiếm,ThànhphốHàNội,Việt
Nam
Email:hotro@kiotviet.com
| TÀI | LIỆU HƯỚNG | DẪN SỬ | DỤNG PUBLIC | API |
| --- | ---------- | ------ | ----------- | --- |
Phiên bản: 4.7.1

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
MỤCLỤC
1.GIỚITHIỆU.................................................................................................................18
2.CHỨCNĂNG...............................................................................................................20
2.1.Authenticate........................................................................................................20
2.2.LấythôngtinAccessToken...................................................................................21
2.3.Nhómhàng..........................................................................................................22
2.3.1.Lấydanhsáchnhómhàng..............................................................................22
2.3.2.Lấychitiêtnhómhàng...................................................................................24
2.3.3.Thêmmớinhómhàng....................................................................................25
2.3.4.Cậpnhậtnhómhàng......................................................................................26
2.3.5.Xóanhómhàng..............................................................................................27
2.4.Hànghóa.............................................................................................................27
2.4.1.Lấydanhsáchhànghóa.................................................................................27
2.4.2.Lấychitiếthànghóa..............................................................................................35
2.4.3.Thêmmớihànghóa..............................................................................................40
2.4.4.Cậpnhậthànghóa................................................................................................43
2.4.5.Xóahànghóa........................................................................................................45
2.4.6.Lấythôngtinthuộctínhsảnphẩm.........................................................................46
2.4.7Thêmmớidanhsáchhànghóa................................................................................46
2.4.8Cậpnhậtdanhsáchhànghóa.................................................................................48
2.4.9Lấydanhsáchtồnkhohànghóa.............................................................................49
2.5.Đặthàng..................................................................................................................50
2.5.1.Lấydanhsáchđặthàng..................................................................................51
2.5.2.Lấychitiếtđặthàng.......................................................................................54
2.5.3.Thêmmớiđặthàng........................................................................................58
2.5.4.Cậpnhậtđặthàng..........................................................................................64
2.5.5.Xóađặthàng.................................................................................................70
2.6.Kháchhàng..............................................................................................................70
2.6.1.Lấydanhsáchkháchhàng..............................................................................70
2.6.2.Lấychitiếtkháchhàng...................................................................................72
2.6.3.Thêmmớikháchhàng....................................................................................74
2.6.4.Cậpnhậtkháchhàng......................................................................................76
2.6.5.Xóakháchhàng.............................................................................................77
2.6.6Thêmmớidanhsáchkháchhàng.....................................................................78
2.6.7Cậpnhậtdanhsáchkháchhàng.......................................................................78
CôngTyCổphầnCôngnghệKiotViet 2/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
2.7.Lấydanhsáchchinhánh...........................................................................................79
2.8.Lấydanhsáchngườidùng........................................................................................81
2.9.Lấydanhsáchtàikhoảnngânhàng...........................................................................82
2.10.Thukhác................................................................................................................83
2.10.1.Lấydanhsáchthukhác.................................................................................83
2.10.2.Thêmmớithukhác......................................................................................84
2.10.3.Cậpnhậtthukhác........................................................................................85
2.10.4.Ngừnghoạtđộngthukhác...........................................................................86
2.11.Webhook...............................................................................................................87
2.11.1.ĐăngkýWebhook........................................................................................87
2.11.2.HuỷđăngkýWebhook.................................................................................89
2.11.3.Kháchhàng..................................................................................................90
2.11.4.Hànghóa.....................................................................................................91
2.11.5.Tồnkho.......................................................................................................93
2.11.6.Đặthàng......................................................................................................94
2.11.7.Hóađơn......................................................................................................96
2.11.8.Bảnggiá......................................................................................................99
2.11.9.Danhmụchànghóa...................................................................................101
2.11.10.Chinhánh................................................................................................102
2.11.11.Danhsáchwebhook.................................................................................103
2.11.12.Chitiếtwebhook......................................................................................104
2.12.Hóađơn..............................................................................................................104
2.12.1.Lấydanhsáchhóađơn...............................................................................105
2.12.2.Lấychitiếthóađơn....................................................................................110
2.12.3.Thêmmớihóađơn....................................................................................114
2.12.4.Cậpnhậthóađơn......................................................................................119
2.12.5.Xóahóađơn..............................................................................................124
2.13.Nhómkháchhàng................................................................................................127
2.13.1.Lấydanhsáchnhómkháchhàng.................................................................127
2.14.Sổquỹ.................................................................................................................128
2.14.1.Lấydanhsáchsổquỹ..................................................................................128
2.14.2.Thanhtoánhóađơn...................................................................................130
2.15.Nhậphàng...........................................................................................................131
2.15.1.Lấydanhsáchnhậphàng............................................................................131
2.15.2.Lấychitiếtnhậphàng................................................................................133
CôngTyCổphầnCôngnghệKiotViet 3/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
2.15.3.Thêmmớinhậphàng.................................................................................135
2.15.4.Cậpnhậtnhậphàng...................................................................................138
2.15.5.Xóanhậphàng...........................................................................................141
2.16.Chuyểnhàng........................................................................................................142
2.16.1.Lấydanhsáchchuyểnhàng........................................................................142
2.16.2.Lấychitiếtchuyểnhàng.............................................................................143
2.16.3.Thêmmớichuyểnhàng..............................................................................144
2.16.4.Cậpnhậtchuyểnhàng................................................................................146
2.16.5.Xóaphiếuchuyểnhàng..............................................................................148
2.17.Bảnggiá...............................................................................................................149
2.17.1.Lấydanhsáchbảnggiá...............................................................................149
2.17.2.Lấychitiếtbảnggiá....................................................................................151
2.17.3.Cậpnhậtchitiếtbảnggiá...........................................................................152
2.18.Kênhbánhàng.....................................................................................................152
2.18.1.Lấydanhsáchkênhbánhàng.....................................................................152
2.19.Trảhàng..............................................................................................................153
2.19.1.Lấydanhsáchtrảhàng...............................................................................153
2.19.2.Lấychitiếtphiếutrảhàng..........................................................................156
2.20.Đặthàngnhập.....................................................................................................158
2.20.1.Lấydanhsáchđặthàngnhập......................................................................158
2.20.2.Lấychitiếtđặthàngnhập...........................................................................161
2.21.Lấydanhsáchlocation.........................................................................................164
2.22.Thiếtlậpcửahàng................................................................................................165
2.23.CậpnhậttrạngtháiCoupon..................................................................................165
2.24.Voucher...............................................................................................................166
2.24.1.Lấydanhsáchđợtpháthành......................................................................166
2.24.2.Lấydanhsáchvouchertrongđợtpháthành................................................168
2.24.3.Tạomớivoucher........................................................................................169
2.24.4.Pháthànhvoucher.....................................................................................169
2.24.5.Hủyvoucher..............................................................................................170
2.25.Thươnghiệu........................................................................................................171
2.25.1.Lấydanhsáchthươnghiệu.........................................................................171
2.26.Nhàcungcấp.......................................................................................................171
2.26.1.Lấydanhsáchnhàcungcấp........................................................................171
2.26.2.Lấychitiếtnhàcungcấp............................................................................174
CôngTyCổphầnCôngnghệKiotViet 4/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
RevisionHistory
| Ngày       | Version | Nộidungthayđổi     |
| ---------- | ------- | ------------------ |
| 16/02/2017 | 1.0     | Tạophiênbảnđầutiên |
Cậpnhật:
|     |     |  Mục2.Chứcnăng,cậpnhật“Authorization”:Bearer{MãAccess |
| --- | --- | ------------------------------------------------------ |
Token}trongheadercủacácrequest.
|     |     |  Mục2.4.3.Thêmmớihànghóa,trongReqest: |
| --- | --- | -------------------------------------- |

Xóa"fullName","categoryName","basePrice","weight",
"images"
 Thêm"masterUnitId","conversionValue"
 Xóa "productId", "productCode","productName"trong
"inventories[]"
|            |     |  Mục2.4.4.Cậpnhậthànghóa,trongRequest: |
| ---------- | --- | --------------------------------------- |
| 21/06/2017 | 1.1 |  Thêm"branchId",                       |
 Xóatrường"fullName","categoryName"

Xóa "productId", "productCode","productName"trong
"inventories[]"
|     |     |  Mục2.5.3.Thêmmớiđặthàng,trongRequest: |
| --- | --- | --------------------------------------- |
 Thêm"totalPayment","accountId","makeInvoice"
 Thêm"locationId",partnerDeliveryId"trong"orderDelivery[]"
 Xóa"payments[]"
|     |     |  Mục2.5.4.Cậpnhậtđơnđặthàng,trongRequest: |
| --- | --- | ------------------------------------------ |
 Thêm"totalPayment","accountId","makeInvoice"

Xóa"payments[]"
Thêm:
|     |     |  ThêmMục2.12cungcấpcácAPIchohóađơn. |
| --- | --- | ------------------------------------ |
Cậpnhật:
1.2
| 31/07/2017 |     |  Mục2.5.1.Lấydanhsáchđặthàng: |
| ---------- | --- | ------------------------------ |
 Thêmthamsố“customerCode","toDate"
 Thêm"customerCode”,“createdDate”trongresponse

Mục2.5.2.Lấychitiếtđặthàng:
CôngTyCổphầnCôngnghệKiotViet 5/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
 Thêm“createdDate”trongresponse
 Mục2.11.6.Đặthàngvà2.11.7.Hóađơn
 Thêm“customerCode”
Thêm:
 ThêmMục2.13cungcấpcácAPIchonhómkháchhàng.
Cậpnhật:
 Mục2.6.1.Lấydanhsáchkháchhàng:
 Thêmthamsố“includeCustomerGroup"trongrequest
 Thêmthamsố“groups”trongresponse
 Mục2.6.2.Lấychitiếtkháchhàng
06/04/2018 1.3
 Thêmthamsố“groups”trongresponse
 Mục2.6.3.Thêmmớikháchhàng
 Thêmthamsố“groupIds”trongrequest
 Thêmthamsố“customerGroupDetails”trongresponse
 Mục2.6.4.Cậpnhậtkháchhàng
 Thêmthamsố“groupIds”trongrequest
 Thêmthamsố“groups”trongresponse
Cậpnhật:
 Mục2.4.2.Lấychitiếthànghóa:
 ThêmAPIlấychitiếttheoCode
 Thêmthamsố“code”trongrequest
 Mục2.5.2.Lấychitiếtđặthàng
 ThêmAPIlấychitiếttheoCode
 Thêmthamsố“code”trongrequest
18/04/2018 1.4  Mục2.6.2.Lấychitiếtkháchhàng
 ThêmAPIlấychitiếttheoCode
 Thêmthamsố“code”trongrequest
 Mục2.12.1.Lấydanhsáchhóađơn
 Thêmthamsố“orderId”trongrequest
 Mục2.12.2.Lấychitiếthóađơn
 ThêmAPIlấychitiếttheoCode
 Thêmthamsố“code”trongrequest
CôngTyCổphầnCôngnghệKiotViet 6/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Thêm:
 Thêmmục2.14cungcấpcácAPIchosổquỹ
 Thêmmục2.14.1:Lấydanhsáchsổquỹ
Cậpnhật:
 Mục2.6.4.Cậpnhậtkháchhàng
 Thêmthamsố“taxCode”trongrequest
 Mục2.5.1.Lấydanhsáchđặthàng
 Thêmthamsố“createdDate”trongrequest
 Mục2.12.1.Lấydanhsáchhóađơn
 Thêmthamsố“createdDate”trongrequest
 Mục2.4.1.Lấydanhhànghóa
16/07/2018 1.5  Thêmthamsố“createdDate”trongresponse
 Mục2.4.2.Lấychitiếthànghóa
 Thêmthamsố“createdDate”trongresponse
 Mục2.12.1,2.12.2: Lấydanhsáchhóađơn
 Thêmthamsố“status”,“statusValue”trong“invoiceDelivery”
(trạngtháivậnđơn)
 Mục2.12.3,2.12.4:Thêmmới,cậpnhậthóađơn
 Thêmthamsố“status”trong“deliveryDetail”(trạngtháivận
đơn)
 Mục2.11.7:Hóađơn(Webhook)
 Thêmthamsố“status”,“statusValue”trong“invoiceDelivery”
(trạngtháivậnđơn)
Thêm:
 Thêmmục2.10.1:Thêmmớithukhác
 Thêmmới2.10.2:Cậpnhậtthukhác
 Thêmmới2.10.3:Ngừnghoạtđộngthukhác
30/07/2018 1.6
Cậpnhật:
 Mục2.6.1.Lấydanhsáchkháchhàng;Mục2.6.2.Lấychitiếtkhách
hàng
 Thêmthamsố“RewardPoint”trongresponse
 Mục2.5.1.Lấydanhsáchđặthàng;Mục2.5.2.Lấychitiếtđặthàng;
CôngTyCổphầnCôngnghệKiotViet 7/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Mục2.12.1.Lấydanhsáchhóađơn;Mục2.12.2.Lấychitiếthóađơn
 Thêmthamsố“Note”trongresponse
 Mục2.4.4.Cậpnhậthànghóa
 Thêmthamsố“IsActive”trongrequest
 Thêmthamsố“IsRewardPoint”trongrequest
 Mục2.5.3.Thêmmớiđặthàng;Mục2.5.4.Cậpnhậtđặthàng;
Mục2.12.3.Thêmmớihóađơn
 Thêmmớithamsố“Surchages”trongrequest
 Mục2.5.2.Lấychitiếtđặthàng;Mục2.5.3.Thêmmớiđặthàng;
Mục2.5.4.Cậpnhậtđặthàng
 Thêmmớithamsố“InvoiceOrderSurcharges”trongresponse
Cậpnhật:
 Mục2.4.1lấydanhsáchhànghóa
 Thêmthamsố“orderTemplate”trongresponse
 Mục2.12.1Lấydanhsáchhóađơn;Mục2.12.2Lấychitiếthóađơn
 Thêmthamsố“SaleChannel”trongresponse
 Mục2.6.1Lấydanhsáchkháchhàng
 Thêmthamsốđểlọckháchhàngtheongàysinhnhật
 Mục2.4Cậpnhậthànghóathêmthamsốmới:
11/03/2019 1.7  Thêmthamsố“minQuantity”(địnhmứctồnnhỏnhất)trong
response
 Thêmthamsố“maxQuantity”(địnhmứctồnnhiềunhất)trong
response
Thêm:
 Mục2.15Phiếunhậphàng:
 Lấydanhsáchphiếunhậphàng
 Lấychitiếtphiếunhậphàng
 Mục2.4.6ThêmAPIlấythôngtinthuộctínhsảnphẩm
Cậpnhật:
25/07/2019 1.8  Mục2.4.1Lấydanhsáchhànghóa
 Thêmthamsố“productType”trongrequest.
CôngTyCổphầnCôngnghệKiotViet 8/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
 Thêmthamsố“includeMaterial”trongrequest.
 Thêmthamsố“productFormulas”trongresponse
 Mục2.4.2Lấychitiếthànghóa
 Thêmthamsố“productFormulas”trongresponse
 Mục2.5.3Thêmmớiđặthàng
 Thêmthamsố“saleChannelId”trongrequest
 Thêmthamsố“saleChannelId”trongresponse
 Mục2.5.4Cậpnhậtđặthàng
 Thêmthamsố“saleChannelId”trongrequest
 Thêmthamsố“saleChannelId”trongresponse
 Mục2.12.3Thêmmớihóađơn
 Thêmthamsố“saleChannelId”trongrequest
 Thêmthamsố“saleChannelId”trongresponse
 Mục2.12.4Cậpnhậthóađơn
 Thêmthamsố“saleChannelId”trongrequest
 Thêmthamsố“saleChannelId”trongresponse
Mục 2.15.1 Lấy danh sách nhập hàng, 2.15.2 Lấy chi tiết
nhậphàng
 Thêmthamsố“supplierCode”trongresponse
 Mục2.4.1Lấydanhsáchhànghóa,2.4.2Lấychitiếthànghóa:
 Thêmthamsố“isLotSerialControl”trongresponse
 Thêmthamsố“IsBatchExpireControl”trongresponse
 Thêmthamsố“productSerials”trongresponse
 Thêmthamsố“productBatchExpires”trongresponse
 Mục2.12.1Lấydanhsáchhóađơn,2.12.2Lấychitiếthóađơn,
2.15.1Danhsáchnhậphàng,2.15.2Chitiếtnhậphàng:
 Thêmthamsố“serialNumbers”trongresponse
 Thêmthamsố“productBatchExpire”trongresponse
Thêm:
 Mục2.16Bảnggiá:
 Lấydanhsáchbảnggiá
 Lấychitiếtbảnggiá
CôngTyCổphầnCôngnghệKiotViet 9/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
 Mục2.17Kênhbánhàng:
 Lấydanhsáchkênhbánhàng
 Mục2.4.7Thêmmớidanhsáchhànghóa
 Mục2.4.8 Cậpnhậtdanhsáchhànghóa
 Mục2.6.6Thêmmớidanhsáchkháchhàng
 Mục2.6.7Cậpnhậtdanhsáchkháchhàng
 Mục2.18Trảhàng:
 Thêmmục2.18.1:Lấydanhsáchphiếutrảhàng
 Thêmmục2.18.2:Lấychitiếtphiếutrảhàng
Cậpnhật:
 Mục2.5.3Thêmmớiđặthàng:
 Thêmthamsố“ExpectedDelivery”trongRequest
 Mục2.2.4Cậpnhậtđặthàng:
 Thêmthamsố“ExpectedDelivery”trongRequest
 Mục2.12.1Lấydanhsáchhóađơn
 Thêmthamsố“FromPurchaseDate”và“ToPurchaDate”trong
Request
 Mục2.12.2 Lấychitiếthóađơn
 Thêmthamsố“OrderCode”trongResponse
 Mục2.12.3 Thêmmớihóađơn
21/09/2019 1.9  Thêmthamsố“ExpectedDelivery”trongRequest
 Mục2.4.1Lấydanhsáchhànghóa
 Thêmthamsố“MasterProductId”trongRequest
 Mục2.12.4Cậpnhậthóađơn
 Thêmthamsố“ExpectedDelivery”trongRequest
 Mục2.5.3Thêmmớiđặthàng:
 Thêmthamsố“Note”trongRequest
 Mục2.12.3 Thêmmớihóađơn
 Thêmthamsố“Note”trongRequest
 Mục2.4.2.Lấychitiếthànghóa
 Thêmthamsố“type”trongresponse
 Mục2.5.3.Thêmmớiđặthàng
CôngTyCổphầnCôngnghệKiotViet 10/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
 Thêmthamsố“partner”trongrequestheader
Thêm:
 Mục2.19Đặthàngnhập:
 LấydanhsáchĐặthàngnhập
 LấychitiếtĐặthàngnhập
 Mục2.2Danhsáchlocation:
 Lấydanhsáchlocation
 Thêmmục2.21cungcấpcácAPIchothiếtlậpcửahàng
 Mục2.6.3Thêmmớikháchhàng
 Thêmthamsố“type”trongresponse
Cậpnhật:
 Mục2.6.1Lấydanhsáchkháchhàng
 ThêmthamsốvàtrảvềthôngtinPsidfacebookfanpagecủa
30/10/2019 2.0
kháchhàng
 Mục2.6.2Lấychitiếtkháchhàng
 TrảvềthôngtinPsidfacebookfanpagecủakháchhàng
14/10/2020 2.1 CậpnhậtlạiURL:https://public.kiotapi.com/surchages
Mục 2.5 và 2.6 sửa lại tên biến “comment” => “comments” cho
12/01/2021 2.1.1
đốitượngkháchhàng
Bổ sung thêm trường “barCode” trong API Lấy danh sách hàng
20/01/2021 2.1.2
hóa,lấychitiếthànghóa,thêmmới/cậpnhậthànghóa.
Bổsungthêm:
2.15.3.Thêmmớinhậphàng
04/06/2021 2.2
2.15.4.Cậpnhậtnhậphàng
2.15.5.Xóanhậphàng
Sửa1sốlỗisai vàbỏthôngtinthừa
17/6 2.2.1
“barCode”:string,//Mãvạchhànghóa(Tốiđa16kýtự)trang23,
27,32,33,35,36
CôngTyCổphầnCôngnghệKiotViet 11/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
| “usingCod”” | bool, | // Có | tạo phiếu | giao | hàng | không? | trang | 50, |
| ----------- | ----- | ----- | --------- | ---- | ---- | ------ | ----- | --- |
trang55
-“usingPriceCod”:bool,/Cóthuhộhaykhông?Trang51,trang56
-“description”:string,/Trang97,102
| -“status”: | byte, (1:Chờxửlý, |     | 2:Đanggiao |     | hàng) | //trạng | thái | vận |
| ---------- | ----------------- | --- | ---------- | --- | ----- | ------- | ---- | --- |
đơntrang98,100
| - “status”: | byte, (1: | Chờ | xử lý, | 2: Đang | giao | hàng,3: | Giao | thành |
| ----------- | --------- | --- | ------ | ------- | ---- | ------- | ---- | ----- |
công,4:Đangchuyểnhoàn,5:Đãchuyểnhoàn,6:Đãhủy,7:Đanglấyhàng,
8:Chờ lấy lại, 9:Đã lấy hàng, 10:Chờ giao lại, 11:Chờ chuyển hàng, 12:Chờ
chuyểnhoànlại)//trạngtháivậnđơntrang103,105
| - “includeInvoiceDelivery”: |     |     | Boolean, | //hóa | đơn | có giao | hàng | hay |
| --------------------------- | --- | --- | -------- | ----- | --- | ------- | ---- | --- |
khôngtrang88
| - “branchId”: | int, |     | // Id chi | nhánh | (Không | cập | nhật | trường |
| ------------- | ---- | --- | --------- | ----- | ------ | --- | ---- | ------ |
này)trang118
-"serialNumbers":string,//Danhsáchimei
"productBatchExpire":{
| "id":long,          |     | //Idlô |             |     |     |     |     |     |
| ------------------- | --- | ------ | ----------- | --- | --- | --- | --- | --- |
| "productId":long,   |     |        | //IDsảnphẩm |     |     |     |     |     |
| "batchName":string, |     |        | //Tên       |     |     |     |     |     |
"fullNameVirgule":string,//Tênđầyđủ
"createdDate":DateTime,//Ngàytạolô
| "expireDate":DateTime |     |     | //Ngàyhếthạnlô |     |     |     |     |     |
| --------------------- | --- | --- | -------------- | --- | --- | --- | --- | --- |
}
| Tạo phiếu | nhập chưa | hỗ  | trợ hàng | hóa | IMEI, | lô date, | thông | tin |
| --------- | --------- | --- | -------- | --- | ----- | -------- | ----- | --- |
responedưtrang117,120
| -   |     |     | Thay |     |     |     |     | đổi |
| --- | --- | --- | ---- | --- | --- | --- | --- | --- |
https://public.kiotapi.com/purchaseorders?id={Id}?IsVoidPayment=true
thành
CôngTyCổphầnCôngnghệKiotViet 12/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
https://public.kiotapi.com/purchaseorders?id={Id}&IsVoidPayment=true
trang121
Hoànthiệnnốtver2.2.1
23/6 2.2.2
Bổsung:
Mục1:lưuývềcáctrườngkhôngbắtbuộc
 Bổsung:
2.4.1Lấydanhsáchhànghóa:thêmmụcgetthôngtinbảohành
26/08/21 2.2.3
bảotrì
2.12.3và2.12.4:thêmmôtảchotrườngwardName
 Bổsung2.16–Chuyểnhàng
04/10/2021 3.0
 2.5.3và2.5.4thêmwardName
18/10/2021 3.1  Bổsung:2.16.4Cậpnhậtchuyểnhàng
2.12.3Thêmmớihóađơn
28/10/2021 3.2
 BổsungthôngtinSerial/Imeikhitạohóađơn
 2.4.9Lấydanhsáchtồnkhohànghóa
02/03/2022 3.3  2.12.3Thêmmớihóađơn
ThêmhàmtạomớiKháchhàng
 Thêm"includeSoftDeletedAttribute"
Lấydanhsáchhànghóa
23/03/2022 3.4
Lấychitiếthànghóa
 Cậpnhậtthêmcácthamsốtrongdanhsáchhànghóa
 ThêmphươngthứcthanhtoánvoucherkhiĐặthàng
2.5.3Thêmmớiđặthàng
31/03/2022 3.5  BổsungthêmtrườngSerial/Imeitrongmục
2.4.3Thêmmớihànghóa
 BổsungbranchIDtrong:2.6.3
21/06/2022 3.6  Sửalạilinklấydanhsáchtồnkhohànghóa:2.4.9
 Cậpnhậttrạngtháivậnđơntrongthêmmới/cậpnhậthóa
CôngTyCổphầnCôngnghệKiotViet 13/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
đơn:2.12.3,2.12.4
2.16.1Lấydanhsáchchuyểnhàng
Bổsung:
“currentItem”:int?,//LấydữliệutừbảnghicurrentItem,
“fromReceivedDate”:DateTime?,//Từthờigiannhận
chuyểnhàng,
“toReceivedDate”:DateTime?,//Đếnthờigiannhận
05/07/2022 3.7
chuyểnhàng,
“fromTransferDate”:DateTime?,//Từthờigianchuyển
hàng,
“toTransferDate”:DateTime?,//Đếnthờigianchuyển
hàng,
2.Chứcnăng
Bổsung:
Lưuý:VớicáchàmGETsẽgiớihạn5000request/1h
2.4.1Lấydanhsáchhànghóa
21/12/2022 3.8
-bổsung:includeWarranties-Lấythôngtinbảohành
- bỏ:"status":int,//0:Lôtạm,1:lôhoànthành
2.4.2Lấychitiếthànghóa
-bỏ:"status":int,//0:Lôtạm,1:lôhoànthành
Bổsungthêm:
17/01/2023 4.0
2.17.3.Cậpnhậtchitiếtbảnggiá
Cậpnhậtthêmtàiliệuwebhook:
2.11.8.Bảnggiá
20/02/2023 4.1
2.11.9.Danhmụchànghóa
2.11.10.Chinhánh
Cậpnhậtmôtảparam:
2.Chứcnăng
09/03/2023 4.2
“Retailer”:têngianhàng
2.19.1.Lấydanhsáchtrảhàng
“statusValue”:string,//trạngtháiđơntrảhàngbằngchữ
CôngTyCổphầnCôngnghệKiotViet 14/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
2.15.3.Thêmmớinhậphàng
Request:
“valueRatio”: decimal?, //Phần trăm thu (nếu truyền cả 2 giá trị
valuevàvalueRatiothìsẽlấyvalueRatio)
Bổsungthêm:
2.11.11.Danhsáchwebhook
2.11.12.Chitiếtwebhook
Bổsungthêm:
2.23.CậpnhậttrạngtháiCoupon
05/06/2023 4.3
Cậpnhậtmục2.4.3.Thêmmớihànghóa:
 Respon không trả về trường "onHand": double? // Tồn kho theo
chinhánh
Bổsungthêm
2.24Voucher
Cậpnhậtcácmục
 2.5.1.Lấydanhsáchđặthàng:
Responsebổsungthêmtrường
“isMaster”: boolean, //Tính năng thêm dòng, true: hàng hóa ở
dòngchính,false:hànghóaởdòngphụ.
 2.5.2.Lấydanhchitiếtđặthàng:
Responsebổsungthêmtrường“isMaster”:Boolean
 2.5.3.Thêmmớiđặthàng:
Bổsungthêmtrường“isMaster”:Boolean
 2.5.3.Cậpnhậtđặthàng:
Bổsungthêmtrường“isMaster”:Boolean
 2.6.1.Lấydanhsáchkháchhàng:
Respontrảvềtrường“wardName”:string,//Phườngxã
28/09/2023 4.4
 2.6.2.Lấychitiếtkháchhàng:
Respontrảvềtrường“wardName”:string,//Phườngxã
 2.6.3.Thêmmớikháchhàng:
Bổsungthêmtrường:
“locationName”:string,//Khuvực
“wardName”:string,//Phườngxã
 2.6.4.Cậpnhậtkháchhàng:
Bổsungthêmtrường:
“locationName”:string,//Khuvực
“wardName”:string,//Phườngxã
 2.6.6.Thêmmớidanhsáchkháchhàng:
Bổsungthêmtrường:
“locationName”:string,//Khuvực
“wardName”:string,//Phườngxã
 2.6.7.Cậpnhậtdanhsáchkháchhàng:
Bổsungthêmtrường:
“locationName”:string,//Khuvực
CôngTyCổphầnCôngnghệKiotViet 15/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“wardName”:string,//Phườngxã
 2.15.1.Lấydanhsáchnhậphàng:
Requestbổsungthêmtrường:
"fromPurchaseDate":"date",optinal//từngàynhậphàng
"toPurchaseDate":"date",optinal//đếnngàynhậphàng
 2.19.1.Lấydanhsáchtrảhàng:
Requestbổsungthêmtrường:
"fromReturnDate":"date",optinal//từngàytrảhàng
"toReturnDate":"date",optinal//đếnngàytrảhàng
 2.12.3.Thêmmớihóađơn:
Requestbổsungthêmtrường:
“isApplyVoucher”: true, //Có apply voucher khi tạo hóa đơn
không
“Payments”:[//Thêmphươngthứcthanhtoánbằngvoucher
"Method":"Voucher",//GiátrịmặcđịnhlàVoucher(khôngđổi)
30/10/2023 4.5 "MethodStr": "Voucher", // Giá trị mặc định là Voucher (không
đổi)
"Amount":50000,//Giátrịcủavoucher
"Id":-1,//Giátrịmặcđịnhlà-1(khôngđổi)
"AccountId":null,//Giátrịmặcđịnhlànull(khôngđổi)
"VoucherId":30996,//Idcủavoucher
"VoucherCampaignId":30087//Idcủađợtpháthànhvoucher
 2.25.ThêmmớiAPIthươnghiệu
 2.4.1Lấydanhsáchhànghóa:
29/02/2024 4.6
Bổsungthêmfiltergethànghóatheothươnghiệu
 2.4.2Lấychitiếthànghóa
Trảthêmthôngtin“tradeMarkId”:int?,//Idthươnghiệu
22/05/2024 4.6.1
Cậpnhậtthôngtincôngtytrêntàiliệu
2.4.1.Lấydanhsáchhànghóa
26/06/2024 4.6.2
- Request: bổ sung param "BranchIds": []int, // Id chi nhánh cần xem tồn
kho
2.11.1.ĐăngkýWebhook
02/08/2024 4.6.3 -Request:bổsungparam“Secret”:string //Mãbímật(khôngbắtbuộcsử
dụng)
-Thôngtinvàcáchsửdụngmãbímật
13/08/2024 4.6.4 2.11.1.ĐăngkýWebhook
-Cậpnhậtthôngtinvàcáchsửdụngmãbímật
Bỏparam“reserved”trongrequestcủacácAPI:
-2.4.3.Thêmmớihànghóa
27/08/2024 4.6.5
-2.4.4.Cậpnhậthànghóa
-2.4.7.Thêmmớidanhsáchhànghóa
-2.4.8.Cậpnhậtdanhsáchhànghóa
13/09/2024 4.6.6 2.22.Thiếtlậpcửahàng
Responsetrảvềthêmparam:
CôngTyCổphầnCôngnghệKiotViet 16/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
- “AllowSellWhenOutStock“: bool // Bán hàng, Chuyển hàng, Trả hàng
nhập,Sảnxuất,Xuấthủykhihếttồnkho
2.5.3.Thêmmớiđặthàng
Requestthêmparam:
- “cashierId”: long?, // ID người tạo đơn đặt hàng, nếu không truyền thì
mặcđịnhAdminlàngườitạo
2.5.4.Cậpnhậtđặthàng
Requestthêmparam:
- “cashierId”: long?, // ID người tạo đơn đặt hàng, nếu không truyền thì
mặcđịnhAdminlàngườitạo
08/11/2024 4.6.7 Bổsungthêm:
2.14.2.Thanhtoánhóađơn
CậpnhậtAPI2.4.1.Lấydanhsáchhànghóa:
-Responsetrảvềthêmparam:
"reserved":double,//Đặthàngtheochinhánh
"minQuality":double,//Địnhmứctồnthấpnhất
4.7.0
20/02/2025
"maxQuality":double,//Địnhmứctồncaonhất
ThêmmớiAPI2.26.Nhàcungcấp:
-2.26.1.Lấydanhsáchnhàcungcấp
-2.26.2.Lấychitiếtnhàcungcấp
25/03/2025 4.7.1 2.11.Webhook
-Cậpnhậtgiảmthờigianphảnhồiwebhookxuống5s
CôngTyCổphầnCôngnghệKiotViet 17/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
1.
GIỚITHIỆU
KiotViet Public API được phát triển để hỗ trợ việc tích hợp và trao đổi dữ liệu giữa KiotViet
vàcácnềntảngwebsite,thươngmạiđiệntử,CRM…
KiotVietPublicAPIcungcấpcơchếđọcvàghicácđốitượngsau:
 Nhómhàng:lấydanhsáchnhómhànghóavớicácthôngtinvềtênnhómhàngvàquanhệ
giữacácnhómhàng(2.3)
 Hànghóa:lấythôngtinsảnphẩm,tạomới,sửa,xóasảnphẩm,thuộctínhcủasảnphẩm(2.4)
 Đặthàng:lấythôngtinđơnhàng,tạođơnhàng,cậpnhậtvàhủyđơnhàng(0)
 Hóađơn:lấythôngtinhóađơn,tạohóađơn,cậpnhậtvàhủyhóađơn(2.12)
 Kháchhàng:lấydanhsáchkháchhàngvàthaotáctrênthôngtinkháchhàng(2.6)
 Phiếunhậphàng:thôngtinphiếunhập(2.15)
 CácAPIphụtrợ
- Danhsáchchinhánh(0)
- Danhsáchngườidùng(2.8)
- Danhsáchtàikhoảnngânhàng(2.9)
- Danhsáchthukhác(2.10)
- Webhook(2.11)
- Nhómkháchhàng(2.13)
- Sổquỹ(2.14)
Lưuý: CácParamscó?ởtronggiátrịlànhữngtrườngcóthểkhôngtruyền
CôngTyCổphầnCôngnghệKiotViet 18/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
CôngTyCổphầnCôngnghệKiotViet 19/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
2.
CHỨCNĂNG
MụcnàymôtảthôngtinchitiếtcủatừngAPI.Cácthôngtinbaogồm:
 TênAPI
 MụcđíchsửdụngcủaAPI
 CấutrúccủaAPI
 Chitiếtthamsốtrongrequest
 Nộidungresponsetrảvề
Chúý:NgoạitrừAPIlấythôngtinAuthenticationCodevàAccessToken,toànbộcácAPIkhác
đềucóheadertrongrequestvớithôngtin:
 “Retailer”:têngianhàng
 “Authorization”:Bearer{MãAccessToken}
Vídụ:
Retailer:taphoa
Authorization: Bearer
eyJhbGciOiJSU0EtT0FFUCIsImVuYyI6IkExMjhDQkMtSFMyNTYiLCJraWQiOiJzQngifQ.h9rN-fArDF-
aL0fkpnagyp6QD8Bt8shBdvaqciahnrVimtKnV8mSlK2LvClw5CoXbm312jCBXN8Gmn7bUxGzP78gFSOr
GQFB5rlYvisDwpcr3R4aC6IeVsCEoEHnrGvz0_v3fv7mI7YhWCQvcea62Wn5bMtSabTKpj_J9VdKjUwe4V
Pp3UYpQoLN8HreL2gmq9BqQC2QBIO25Mk3yPeaJaXTueFXKjYR-
0f0qSsnw1lEMPRp8ECfq3w0N3CYmc-
lg2zvqYFLBmQqdxlnwjE__6ebRDtXp_qNKy7LmgLaR3LzKIzUHDdFN4fUQ23hZX5HmQ_9xNcEH_Otg1E
BZ5T2Xg.vToCTB4ZmAHWUEjVRg5C0A.Z8UK_2Y-
dEZeZNNO5drADRbZkrkpLG3FaLMFnPFhAc6iEKiMBorOgdm5uZI4FzMGvbfBUuVU5AlbOr0MxSickdhw
Idi1H9pSytHzqAuC2qr_1kvlGkYmr6gz9WAsTWMnPhFQ8DMV5jhNKxYod8zzXUuILdi7eHC2mxAygN_f
Ma04yoFfEp3742of57LLgAqkKKY0ADK_LzJGmkcBbe2x4w.sEiuD4cqFqj9Wj9kOZ31gSjq6REOpMUj3hB
YBojekzw
Lưuý:VớicáchàmGETsẽgiớihạn5000request/1h
2.1. Authenticate
KiotvietAPIxácthựcdựatrêncơchếxácthựcOAuth2.0,đểkếtnốiđượchệthốngcầncó2
thông tin: ClientId và Mã bảo mật. Thông tin này được truy cập vào “Thiết lập cửa hàng” bằng tài
khoảnadmin=>ChọnThiếtlậpkếtnốiAPI
CôngTyCổphầnCôngnghệKiotViet 20/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Trong trường hợp không thể lấy được thông tin trên vui lòng liên hệ với bộ phận CSKH để
đượchỗtrợ.
Sau khi có được thông tin ClientId và Mã bảo mật (client_secret). Có thể sử dụng các thư
việntheotừngngônngữđểlấythôngtinAccessToken,vídụ:
+VớiC#:https://www.nuget.org/packages/OAuth2Client/
+VớiPHP:https://github.com/thephpleague/oauth2-client
Thôngtinendpointauthenticatenhưsau:
- AuthorizationEndpoint:https://id.kiotviet.vn/connect/authorize
- TokenEndpoint:https://id.kiotviet.vn/connect/token
HoặccóthểcallAPIbêndưới(2.2)
2.2. LấythôngtinAccessToken
Mụcđíchsửdụng:APIlấythôngtinAccessTokenđểtruycập
PhươngthứcvàURL:POSThttps://id.kiotviet.vn/connect/token
Request:
scopes:PublicApi.Access//Phạmvitruycập(PublicAPI)
grant_type:client_credentials//Thôngtintruycậpdạngtoken
client_id:83a5bcbe-3c39-458c-bdd9-128112cef3f7//ClientId
client_secret:3B52F3A9DDE194966DAE2CE0A478B2DEC15254D6//Clientsecret
Header:
"Content-Type":"application/x-www-form-urlencoded"
scope
CôngTyCổphầnCôngnghệKiotViet 21/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Body:
scopes=PublicApi.Access&grant_type=client_credentials&client_id=e4fe37ab-5d10-4919-bf59-
d9a568456d0b&client_secret=01A3703244752CFF6350A801F900742179C7CCDA
Response:
{
"access_token":"",
"expires_in":86400,
"token_type":"Bearer"
}
2.3. Nhómhàng
Môtảchitiếtchocácliênquanđếnthôngtinnhómhànghóanhưsau:
2.3.1. Lấydanhsáchnhómhàng
Mục đích sử dụng: Trả về toàn bộ danh mục hàng hóa (nhóm hàng hóa). Danh sách này
đượcsắpxếptheothứtựbảngchữcái(a-z).Hệthốngchỉchophépnhómhànghóacótốiđa3cấp,
và không cho phép xóa nhóm hàng cha nếu đang có chứa nhóm hàng con và không cho phép xóa
nhómhàngconnếuđangđượcsửdụng.
PhươngthứcvàURL:GEThttps://public.kiotapi.com/categories
Request:SửdụnghàmGETvớithamsố
“lastModifiedFrom”:datetime?//thờigiancậpnhật
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int,//lấydữliệutừbảnghihiệntại,nếukhôngnhậpthìmặcđịnhlà
0
“orderBy”:string,//SắpxếpdữliệutheotrườngorderBy(Vídụ:orderBy=name)
“orderDirection”: string, //Sắp xếp kết quả trả về theo: Tăng dần Asc (Mặc định),
giảmdầnDesc
CôngTyCổphầnCôngnghệKiotViet 22/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“hierachicalData”: Boolean, // nếu HierachicalData=true thì mình sẽ lấy nhóm hang
theocấpmàkhôngquantâmlastModifiedFrom.Ngượclại,HierachicalData=falsethìsẽlấy1
listnhómhangtheolastModifiedFromnhưngkhôngcóphâncấp
Response:
 NếuhierachicalDatalàtrue
“total”:int,
“pageSize”:int,
“data”: [
{
“categoryId”:int,//IDnhómhànghóa
"parentId”:int?,//Nếudanhmụccódanhmụcchathìcóidcụthể,nếukhôngcódanh
mụccha,ParentId=null
"categoryName":string,//Tênnhómhànghóa
“retailerId”:int,//Idcửahàng
“hasChild”:boolean?,//nhómhàngcóconhaykhông
“modifiedDate”:datetime?//thờigiancậpnhật
“createdDate”:datetime
“children”:[]
}],
“removedIds”:int[],//danhsáchIDnhómhàngbịxóadựatrênModifiedDate
"timestamp":datetime
}
 NếuhierachicalDatalàfasle
“total”:int,
“pageSize:”int,
CôngTyCổphầnCôngnghệKiotViet 23/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“data”: [
{
“categoryId”:int,//IDnhómhànghóa
“parentId”:int?,//Nếudanhmụccódanhmụcchathìcóidcụthể,nếukhôngcódanh
mụccha,ParentId=null
"categoryName":string,//Tênnhómhànghóa
“retailerId”:int,//Idcửahàng
“hasChild”:boolean?,//nhómhàngcóconhaykhông
“modifiedDate”:datetime?//thờigiancậpnhật
“createdDate”:datetime
}],
“removedIds”:int[],//danhsáchIDnhómhàngbịxóadựatrênModifiedDate
"timestamp":datetime
}
2.3.2. Lấychitiêtnhómhàng
Mụcđichsửdụng:TrảlạithôngtinchitiếtcủanhómhànghóatheoID
PhươngthứcvàURL:GEThttps://public.kiotapi.com/categories/{id}
Request:SửdụnghàmGETvớithamsố:
“id”:long//IDcủanhómhàng
Response:
“data”:{
“categoryId”:int,//IDnhómhànghóa
“parentId”:int?,//Nếudanhmụccódanhmụccha
“categoryName”:string,//Tênnhómhànghóa
“retailerId”:int,//IDcửahàng
“hasChild”:int?,//IDcửahàng
CôngTyCổphầnCôngnghệKiotViet 24/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“modifiedDate:datetime?,//Thờigiancậpnhật
“createdDate”:datetime,
“children”:[]
}
2.3.3. Thêmmớinhómhàng
Mụcđíchsửdụng:Thêmmớimộtnhómhàng
PhươngthứcvàURL:POSThttps://public.kiotapi.com/categories
Request:JSONmãhóayêucầugồm1objectnhómhàngriêngbiệtvớinhưngthamsốsau:
“categoryName”:string//tênnhómhànghóa
“parentId”:int//nếunhómhàngcónhómhàngcha(hệthốngchophéptốiđa3cấp
nhóm)
Body
{
“categoryName”:string
}
Response:
{
“message”:“Cậpnhậtdữliệuthànhcông”,
“data”:{
“categoryId”:int,//IDnhómhànghóa
“parentId”:int?,//Nếudanhmụccódanhmụccha
“categoryName":string,//Tênnhómhànghóa(Tốiđa125kýtự)
“retailerId”:int,//IDcửahàng
CôngTyCổphầnCôngnghệKiotViet 25/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“hasChild”:boolean?,//Códanhmụccon
“modifiedDate”:datetime?,
“createdDate”:datetime,
“children”:[]
}
}
2.3.4. Cậpnhậtnhómhàng
Mụcđíchsửdụng:CậpnhậtnhómhànghóatheoID
PhươngthứcvàURL:PUThttps://public.kiotapi.com/categories/id
Request:SửdụnghàmPUTvớiIDnhómhàngqua1objectJSON.
“id”:long//IDnhómhànghóa
Body
{
"parentId":int,//Nếudanhmụccódanhmụccha
"categoryName":string//Tênnhómhànghóa(tốiđa125kýtự)
}
Response:
{
"message":"Cậpnhậtdữliệuthànhcông",
"data":{
"categoryId":int,//Idnhómhànghóa
"parentId":int,//Nếudanhmụccódanhmụccha
"categoryName":string,//Tênnhómhànghóa(tốiđa125kýtự)
"retailerId":int,//Idcửahàng
CôngTyCổphầnCôngnghệKiotViet 26/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"hasChild":false,//Códanhmụccon
"modifiedDate":datetime,
"createdDate":datetime,
"children":[]
}
}
2.3.5. Xóanhómhàng
Mụcđíchsửdụng:XóanhómhàngtheoID
PhươngthứcvàURL:DELETEhttps://public.kiotapi.com/categories/{id}
Request:RequestsẽbaogồmIdcủanhómhàngtrongURL:
“id”:long//IDcủanhómhàng
Response:Trảlạithôngtinxóathànhcông(Code200)
{
"message":"Xóadữliệuthànhcông"
}
2.4. Hànghóa
Môtảchitiếtchocácliênquanđếnthôngtinhànghóanhưsau:
2.4.1. Lấydanhsáchhànghóa
Mụcđíchsửdụng:Trảvềtoànbộhànghóatheocửahàngđãđượcxácnhận(authenticated
retailer)
PhươngthứcvàURL:GEThttps://public.kiotapi.com/products
Request:SửdụnghàmGETvớithamsố:
CôngTyCổphầnCôngnghệKiotViet 27/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
{
“orderBy”:string,optional//SắpxếpdữliệutheotrườngorderBy(vídụ:orderBy=Name)
“lastModifiedFrom”:datetime?//thờigiancậpnhật
“pageSize”:int,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int,//lấydữliệutừbảnghicurrentItem
“includeInventory”:Boolean,//cólấythôngtintồnkho?
“includePricebook”:Boolean,//cólấythôngtinbảnggiá?
“IncludeSerials”:Boolean,//lấythôngtinserialimei
“IncludeBatchExpires”:Boolean,//lấythôngtinlô,hạnsửdụng
“includeWarranties”:Boolean,//Lấythôngtinbảohành
“masterUnitId”:long?,//Idhànghoáđơnvịcầnfilter
“masterProductId”:long?,//Idhànghoácùngloạicầnfilter
“categoryId”:int?,//Idnhómhàngcầnfilter
“BranchIds”:[]int,//Idchinhánhcầnxemtồnkho
“orderDirection”:string,optional
"includeRemoveIds":bool,//CólấythôngtindanhsáchIdbịxoádựa
"includeQuantity":bool,//cólấythôngtinđịnhmứctồn
"productType":bool,//loạihànghóa
"includeMaterial":bool,//cólấydanhsáchhàngthànhphần
"isActive":bool?//Hàngđangkinhdoanh,
"name":string//searchhànghóatheotên
"includeSoftDeletedAttribute":bool//Cólấythôngtindanhsáchthuộctínhbịxóacủa
hànghóa(mặcđịnhlàtruenếukhôngtruyềnthamsốnày=>Nghĩalàlấytấtcảthuộctínhbaogồm
thuộctínhđãbịxóa.Ngượclạinếu=falsethìloạibỏcácthuộctínhđãbịxóa).
“tradeMarkId”:int?,//Idthươnghiệucầnfilter
}
CôngTyCổphầnCôngnghệKiotViet 28/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Nếucó"OrderDirection",chọnsắpxếpkếtquảvềtheo:
 ASC(Mặcđịnh)
 DESC
“includeRemoveIds”: Boolean //Có lấy thông tin danh sách Id bị xoá dựa trên
lastModifiedFrom,
“productType”:int?(optional)//Loạihànghóa
Nếucó"productType",giátrịthuộc:
 1:hàngcombo
 3:hànghóadịchvụ
 2:cáchànghóacònlại
“includeMaterial”: Boolean //Có lấy thông tin danh sách hàng thành phần hay
không
Response:
“removeId”:int[],//DanhsáchIdhànghóabịxóadựatrênModifiedDate
“total”:int,//Tổngsốhànghóa
“pageSize”:int,
“data”:[{
“id”:long,//IDhànghóa
“code”:string,//Codehànghóa
“barCode”:string,//Mãvạchhànghóa
“retailerId”:int,//Idcửahàng
"allowsSale":Boolean,//Sảnphẩmđượcbántrựctiếphaykhông
"name":string,//Tênsảnphẩm
"categoryId":int,//Idcủanhómhànghóa
"categoryName":string,//Têncủanhómhànghóa
CôngTyCổphầnCôngnghệKiotViet 29/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"tradeMarkId":int,//Idcủathươnghiệu
"tradeMarkName":string,//Têncủathươnghiệu
"fullName":string,//Tênsảnphẩmbaogồmthuộctínhvàđơnvịtính
“description”:string,//Môtảsảnphẩm
"hasVariants":Boolean?,//Sảnphẩmcóthuộctínhhaykhông
"attributes":[{
“productId”:long,//Idsảnphẩm
“attributeName”:string,//tênthuộctính
“attributeValue”:string//giátrịthuộctính
}],//danhsáchthuộctính
“unit”:string,//đơnvịtínhcủa1sảnphẩm,
“masterUnitId”:long,//Idcủahànghóađơnvịcơbản(null)
“masterProductId”:long?,
“conversionValue”:double?,//Đơnvịquyđổi
"units":[{
“id”:long,//IDsảnphẩm
“code”:string,//Mãsảnphẩm
“name”:string,//Tênsảnphẩm
“fullName”:string,//Tênsảnphẩm
“unit”:string,//Đơnvịtính
“conversionValue”:double,//Đơnvịquyđổi
“basePrice”:decimal,//Giábáncủasảnphẩm
}],//danhsáchđơnvịtính
“images”:[{“Image”:string,//ảnhsảnphẩm}],//Danhsáchhìnhảnhcủahànghóa
“inventories”:[{
"productId":long,//Idcủasảnphẩm
CôngTyCổphầnCôngnghệKiotViet 30/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"productCode":string,//Mãcủasảnphẩm
"productName":string,//Têncủasảnphẩm
Catalog
1.GIỚITHIỆU.................................................................................................................18
2.CHỨCNĂNG...............................................................................................................20
2.4.2.Lấychitiếthànghóa..............................................................................................35
2.4.3.Thêmmớihànghóa..............................................................................................40
2.4.4.Cậpnhậthànghóa................................................................................................43
2.4.5.Xóahànghóa........................................................................................................45
2.4.6.Lấythôngtinthuộctínhsảnphẩm.........................................................................46
2.4.7Thêmmớidanhsáchhànghóa................................................................................46
2.4.8Cậpnhậtdanhsáchhànghóa.................................................................................48
2.4.9Lấydanhsáchtồnkhohànghóa.............................................................................49
2.5.Đặthàng..................................................................................................................50
2.6.Kháchhàng..............................................................................................................70
2.7.Lấydanhsáchchinhánh...........................................................................................79
2.8.Lấydanhsáchngườidùng........................................................................................81
2.9.Lấydanhsáchtàikhoảnngânhàng...........................................................................82
2.10.Thukhác................................................................................................................83
2.11.Webhook...............................................................................................................87
2.12.Hóađơn..............................................................................................................104
2.13.Nhómkháchhàng................................................................................................127
2.14.Sổquỹ.................................................................................................................128
2.15.Nhậphàng...........................................................................................................131
2.16.Chuyểnhàng........................................................................................................142
2.17.Bảnggiá...............................................................................................................149
2.18.Kênhbánhàng.....................................................................................................152
2.19.Trảhàng..............................................................................................................153
2.20.Đặthàngnhập.....................................................................................................158
2.21.Lấydanhsáchlocation.........................................................................................164
2.22.Thiếtlậpcửahàng................................................................................................165
2.23.CậpnhậttrạngtháiCoupon..................................................................................165
2.24.Voucher...............................................................................................................166
2.25.Thươnghiệu........................................................................................................171
CôngTyCổphầnCôngnghệKiotViet 31/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
2.26.Nhàcungcấp.......................................................................................................171
"branchName":string,//Têncủachinhánh
"onHand":double?,//Tồnkhotheochinhánh
"cost":decimal?,//Giásảnphẩm
"onOrder":double,//Sốlượngđặttừnhàcungcấptheochinhánh
"reserved":double,//Đặthàngtheochinhánh
"minQuality":double,//Địnhmứctồnthấpnhất
"maxQuality":double,//Địnhmứctồncaonhất
}],//danhsáchtồnkhotrêncácchinhánh
“priceBooks”://bảnggiá(mặcđịnhlàbảnggiáchung)
[{
"priceBookId":long,//IDbảnggiá
"priceBookName":string,//Tênbảnggiá
“productId”:long//IDsảnphẩm
"isActive":Boolean,//Cóđượcsửdụng?
“startDate”:datetime?,//cóhiệulựctừngày
“endDate”:datetime?,//cóhiệulựcđếnngày
“price”:decimal,//Giábántheobảnggiá
}],//danhsáchcácbảnggiámàsảnphẩmđangđượcgán
“productFormulas”://danhsáchhàngthànhphần(nếucó)
[{
"materialId":long,//IDhàngthànhphần
"materialCode":string,//Codehàngthànhphần
“materialFullName”:string//tênđầyđủhàngthànhphần
"materialName":string,//tênhàngthànhphần
CôngTyCổphầnCôngnghệKiotViet 32/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“quantity”:int,//sốlượng
“basePrice”:decimal,//giá
“productId”:long?,//mãhàngcombochứasảnphẩmnày
“product”://chitiếtsảnphẩm
{
“createdDate”:datetime?,//ngàytạo
“id”:long,//IDsảnphẩmcủathànhphần,
“retailerId”:long,//Idcửahàng,
“code”:string,//codesảnphẩmcủathànhphần,
“name”:string?,//tênhàngthànhphần,
“fullName”:datetime?,//tênđầyđủhàngthànhphần,
“categoryId”:int,//Idcủanhómhànghóa,
“allowsSale”:Boolean,//Sảnphẩmđượcbántrựctiếphaykhông,
“hasVariants”:Boolean?,//Sảnphẩmcóthuộctínhhaykhông,
“basePrice”:decimal,//Giábáncủasảnphẩm,
“unit”:string,//Đơnvịtính,
“conversionValue”:double?,//Đơnvịquyđổi,
“modifiedDate”:datetime?,//ngàysửa,
“isActive”:Boolean,//Cóđượcsửdụng?,
“isRewardPoint”:bool?,//cótíchđiểmhaykhông,
“orderTemplate”:string,//Mẫughichú(hóađơnđặthàng),
“isLotSerialControl”:bool?,//Cóphảiimeihaykhông
“isBatchExpireControl”:bool?//CóphảiHànglô/datehaykhông
},
}],
"productSerials"://DanhsáchImei
[{
CôngTyCổphầnCôngnghệKiotViet 33/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"productId":long,//Idsảnphẩm
"serialNumber":string,//sốserialImei
"status":int,//1:cònhàng,0:hếthàng
"branchId":int,//idchinhánh
"quantity":double?,//Sốlượng:1
"createdDate":datetime,//ngàytạo
"modifiedDate":datetime?,//ngàysửa
}],
"productBatchExpires"://Danhsáchlô
[{
"productId":long,//Idsảnphẩm
"onHand":double,//Tồnkhocủalô
"batchName": string,//Tênlô
"expireDate":datetime,//ngàyhếthạnlô
"fullNameVirgule":string,//Tênđầyđủcủalô
"branchId":int,//idchinhánh
}],
}],
"productWarranties":[{
"Id":long,//idbảohànhbảotrì
"description":string,//môtả
"numberTime":int,//thờigianbảohành
"timeType":int,//kiểuthờigian(ngày:1tháng:2năm:3)
"warrantyType":int,//kiểubảohành(bảohành:1,bảotrì:3)
"productId":long,//productId
"retailerId":long,//retailerId
"createdBy":long?,//ngườitạo
"createdDate":datetime?, //thờigiantạo
CôngTyCổphầnCôngnghệKiotViet 34/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"modifiedDate":datetime?,//thờigianthayđổi
}]
“basePrice”:decimal?,//giásảnphẩm
“weight”:double?,//trọnglượngsảnphẩm
“modifiedDate”:datetime//thờigiancậpnhật
“createdDate”:datetime,//thờigiantạo,
“orderTemplate”:string//Mẫughichú(hóađơnđặthàng),
“minQuantity”:int //Địnhmứctồnnhỏnhất
“maxQuantity”:int //Địnhmứctồnnhiềunhất,
}],
2.4.2. Lấychitiếthànghóa
Mụcđíchsửdụng:TrảlạichitiếtcủamộtsảnphẩmcụthểtheoID,theoCode
PhươngthứcvàURL:
- TheoId:GEThttps://public.kiotapi.com/products/{id}
- TheoCode:GEThttps://public.kiotapi.com/products/code{code}
- Cólấythuộctínhđãxóamềmcủahàng
hóa:GEThttps://public.kiotapi.com/products/{id}?includeSoftDeletedAttribute=true
Request:SửdụnghàmGETvớithamsố:
“id”:long//IDcủahànghóa
“code”:string//Mãcủahànghóa
"includeSoftDeletedAttribute":bool//Cólấythôngtindanhsáchthuộctínhbịxóacủahàng
hóa(mặcđịnhlàtruenếukhôngtruyềnthamsốnày=>Nghĩalàlấytấtcảthuộctínhbaogồmthuộc
tínhđãbịxóa.Ngượclạinếu=falsethìloạibỏcácthuộctínhđãbịxóa).
Response:
{
“id”:long,//IDhànghóa
CôngTyCổphầnCôngnghệKiotViet 35/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“code”:string,//Codehànghóa
“barCode”:string,//Mãvạchhànghóa
“retailerId”:int,//IDcửahàng
"allowsSale":Boolean?,//Sảnphẩmđượcbántrựctiếphaykhông
"name":string,//Tênsảnphẩm
"categoryId":int,//IDcủanhómhànghóa
"tradeMarkId":int,//IDcủathươnghiệu
"type":byte?,//Loạihànghóa
"categoryName":string,//Têncủanhómhànghóa
"fullName":string,//Tênsảnphẩmbaogồmunitvàthuộctính?
“description”:string,//Môtảsảnphẩm
"hasVariants":Boolean?,//Sảnphẩmcóthuộctínhhaykhông
"attributes"://Danhsáchthuộctính
[{
“productId”:long,//IDthuộctính
“attributeName”:string,//Tênthuộctính
“attributeValue”:string//Giátrịthuộctính
}],
“unit”:string,//Đơnvịtínhcủa1sảnphẩm,
“masterProductId”:long?,
“masterUnitId”:long,//IDcủahànghóađơnvịcơbản(null)
“conversionValue”:double?,//Đơnvịquyđổi
"units"://Danhsáchđơnvịtính
[{
“id”:long,//IDsảnphẩm
CôngTyCổphầnCôngnghệKiotViet 36/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“code”:string,//Mãsảnphẩm,
“name”:string,//Tênsảnphẩm
“fullName”:string,//Tênsảnphẩmbaogồmthuộctínhvàđơnvịtính
“unit”:string,//Đơnvịtính
“conversionValue”:double,//Đơnvịquyđổi
“basePrice”:decimal,//Giábáncủasảnphẩm
}],
“images”:string[],//Danhsáchhìnhảnhcủahànghóa
“inventories”://Danhsáchtồnkhotrêncácchinhánh
[{
"productId":long,//Idcủasảnphẩm
"productCode":string,//Mãcủasảnphẩm
"productName":string,//Têncủasảnphẩm
"branchId":long,//Idcủachinhánh
"branchName":long,//Têncủachinhánh
"onHand":double?,//Tồnkhotheochinhánh
"cost":decimal?,//Giásảnphẩm
“reserved”:double//Đặthàngtheochinhánh
}],
“priceBooks”://Danhsáchbảnggiá,mặcđịnhcóbảnggiáchung
[{
"priceBookId":long,//IDbảnggiá
"priceBookName":string,//Tênbảnggiá
“productId”:long//IDsảnphẩm
"isActive":Boolean,//Cóđượcsửdụng?
CôngTyCổphầnCôngnghệKiotViet 37/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“startDate”:datetime?,//Cóhiệulựctừngày
“endDate”:datetime?,//Cóhiệulựcđếnngày
“price”:decimal,//Giábántheobảnggiá
}],
“productFormulas”://Danhsáchhàngthànhphần(nếucó)
[{
"materialId":long,//IDhàngthànhphần
"materialCode":string,//Mãhàngthànhphần
“materialFullName”:string//Tênhàngthànhphầnbaogồmthuộctínhvàđơnvị
tính
"materialName":string,//Tênhàngthànhphần
“quantity”:int,//Sốlượng
“basePrice”:decimal,//Giábánhàngthànhphần
“productId”:long?,//IDhàngthànhphần
“product”://Chitiếthàngthànhphần
{
“createdDate”:datetime?,//Ngàytạohànghóa
“id”:long,//IDhàngthànhphần
“retailerId”:long,//IDcửahàng
“code”:string,//Mãhàngthànhphần
“name”:string?,//Tênhàngthànhphần
“fullName”: datetime?, // Tên hàng thành phần bao gồm thuộc tính
vàđơnvịtính
“categoryId”:int,//IDcủanhómhànghóa
“allowsSale”:Boolean,//Sảnphẩmđượcbántrựctiếphaykhông
“hasVariants”:Boolean?,//Sảnphẩmcóthuộctínhhaykhông
CôngTyCổphầnCôngnghệKiotViet 38/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“basePrice”:decimal,//Giábáncủasảnphẩm
“unit”:string,//Đơnvịtính
“conversionValue”:double?,//Đơnvịquyđổi
“modifiedDate”:datetime?,//Ngàygầnnhấtcócậpnhậthànghóa
“isActive”: Boolean, // Trạng thái kinh doanh hàng hóa (true: Đang
kinhdoanh|false:ngừngkinhdoanh)
“isRewardPoint”:bool?,//Cótíchđiểmhaykhông
“orderTemplate”:string//Mẫughichú(hóađơnđặthàng)
}
}],
“basePrice”:decimal,//giásảnphẩm
“weight”:double,//trọnglượngsảnphẩm
“modifiedDate”:datetime,//thờigiancậpnhật
“createdDate”:datetime,//thờigiantạo
“isLotSerialControl”:bool?,//Cóphảiimeihaykhông
“isBatchExpireControl”:bool?,//CóphảiHànglô/datehaykhông
"productSerials": //DanhsáchImei
[{
"productId":long,//Idsảnphẩm
"serialNumber":string,//sốserialImei
"status":int,//1:cònhàng,0:hếthàng
"branchId":int,//idchinhánh
"quantity":double?,//Sốlượng:1
"createdDate":datetime,//ngàytạo
"modifiedDate":datetime?,//ngàysửa
}],
"productBatchExpires"://Danhsáchlô
[{
"productId":long,//Idsảnphẩm
CôngTyCổphầnCôngnghệKiotViet 39/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"onHand":double,//Tồnkhocủalô
"batchName": string,//Tênlô
"expireDate":datetime,//ngàyhếthạnlô
"fullNameVirgule":string,//Tênđầyđủcủalô
"branchId":int,//IDchinhánh
}]
}
2.4.3. Thêmmớihànghóa
Mụcđíchsửdụng:Tạomớihànghóa
PhươngthứcvàURL:POSThttps://public.kiotapi.com/products
Request:JSONmãhóayêucầugồm1objecthànghóa:
{
“name”:string,//Tênhànghóa
“code”:string,//Mãhànghóa
“barCode”:string,//Mãvạchhànghóa,tốiđa16kýtự
“fullName”:string,//Tênsảnphẩmbaogồmthuộctínhvàđơnvịtính
“categoryId”:int,//Idnhómhànghóa
“allowsSale”:Boolean,//Sảnphẩmđượcbántrựctiếphaykhông
“description”:string,//Môtảsảnphẩm
“hasVariants”:boolean,//Sảnphẩmcóthuộctínhhaykhông
“isProductSerial”:true,//Cóphảisảnphẩmserialhaykhông
“attributes”:[{
“attributeName”: string, // Tên thuộc tính (Nếu tên thuộc tính chưa tồn tại
tronghệthốngthìtựđộngtạomớithuộctính)
“attributeValue”:string//Giátrịthuộctính
}],//Danhsáchthuộctính
CôngTyCổphầnCôngnghệKiotViet 40/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“unit”:string,//Đơnvịtínhcủa1sảnphẩm
“masterProductId”:long?,//IDhànghoácùngloại
“masterUnitId”:long?,//IDcủahànghóađơnvịcơbản,=NULLnếulàđơnvịcơbản
“conversionValue”:double,
“inventories”:[{
“branchId”:long,//IDcủachinhánh
“branchName”:long,//Têncủachinhánh
“onHand”:double?,//Tồnkhotheochinhánh
“cost”:decimal?//Giásảnphẩm
}],//Danhsáchtồnkhotrêncácchinhánh
“basePrice”:decimal?,//Giásảnphẩm
“weight”:double?,//Trọnglượngsảnphẩm,
“images”:string[],//Danhsáchhìnhảnhhànghóa(dạnglink)
}
Response:
{
“id”:int,//IDhànghóa
“code”:string,//Mãhànghóa
“barCode”:string,//Mãvạchhànghóa
“name”:string,//Tênhànghóa
“fullName”:string,//Tênhànghóabaogồmthuộctínhvàđơnvịtính
“description”:string,//Tênhànghóa
“images”:string[],//Danhsáchhìnhảnhhànghóa(dạnglink)
“categoryId”:int,
“categoryName”:string,
CôngTyCổphầnCôngnghệKiotViet 41/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“unit”:string,
“masterProductId”:long?,
“masterUnitId”:long,
“conversionValue”:double?,
"hasVariants":Boolean,//Sảnphẩmcóthuộctínhhaykhông
"attributes":[{
“productId”:long,//IDthuộctính
“attributeName”:string,//Tênthuộctính
“attributeValue”:string//Giátrịthuộctính
}]//Danhsáchthuộctính
“basePrice”:decimal,//Giábán
“inventories”:[{
“productId”:long,//IDcủasảnphẩm
“productCode”:string,//Mãcủasảnphẩm
“productName”:string,//Têncủasảnphẩm
“branchId”:long,//IDcủachinhánh
“branchName”:long,//Têncủachinhánh
“onHand”:double?,//Tồnkhotheochinhánh
“cost”:decimal?,//Giásảnphẩm
“reserved”:double//Đặthàngtheochinhánh
}]
“basePrice”:decimal,//Giábántheobảnggiá
“retailerId”:int,//IDcửahàng
“modifiedDate”:datetime,//Thờigiancậpnhật
}
CôngTyCổphầnCôngnghệKiotViet 42/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
2.4.4. Cậpnhậthànghóa
Mụcđíchsửdụng:CậpnhậthànghóatheoID
PhươngthứcvàURL:PUThttps://public.kiotapi.com/products/id
Request:SửdụnghàmPUTvớiIDhànghóaqua1objectJSON.
“branchId”:int,//IDchinhánhhiệntại
“id”:long//IDhànghóa
Body:
{
“name”:string,//Tênhànghóa
“code”:string,//Mãhànghóa
“barCode”:string,//Mãvạchhànghóa,tốiđa16kýtự
“categoryId”:int,//IDnhómhànghóa
"allowsSale":Boolean,//Sảnphẩmđượcbántrựctiếphaykhông
“description”:string,//Môtảsảnphẩm
“hasVariants”:boolean,//Sảnphẩmcóthuộctínhhaykhông
"attributes":[{
“attributeName”: string, // Tên thuộc tính (Nếu tên thuộc tính chưa tồn tại
tronghệthốngthìtựđộngtạomớithuộctính)
“attributeValue”:string//Giátrịthuộctính
}],//Danhsáchthuộctính
“unit”:string,//Đơnvịtínhcủa1sảnphẩm
“masterUnitId”:long,//IDcủahànghóađơnvịcơbản,=NULLnếulàđơnvịcơbản
“conversionValue”:int,//Đơnvịquyđổi,=1nếulàđơnvịcơbản
“inventories”: [{
"branchId":long,//IDcủachinhánh
"branchName":long,//Têncủachinhánh
CôngTyCổphầnCôngnghệKiotViet 43/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"onHand":double?,//Tồnkhotheochinhánh
"cost":decimal?//Giásảnphẩm
}],//Danhsáchtồnkhotrêncácchinhánh
“basePrice”:decimal,//Giásảnphẩm
“weight”:double,//Trọnglượngsảnphẩm,
“isActive”:bool?,//Trạngtháihóađộng(true:đanghoạtđộng|false:ngừnghoạtđộng)
“isRewardPoint”:bool?,//Cótíchđiểmhaykhông
}
Response:
{
“id”:int,//IDhànghóa
“code”:string,//Mãhànghóa
“barCode”:string,//Mãvạchhànghóa
“name”:string,//Tênhànghóa
“fullName”:string,//Tênhànghóabaogồmthuộctínhvàđơnvịtính
“description”:string,//Tênhànghóa
“images”:string[],//Danhsáchhìnhảnhhànghóa(dạnglink)
“categoryId”:int,
“categoryName”:string,
“unit”:string,
“masterUnitId”:long,
“conversionValue”:double,
“hasVariants”:boolean,//Sảnphẩmcóthuộctínhhaykhông
"attributes":[{
“attributeName”:string,//Tênthuộctính
CôngTyCổphầnCôngnghệKiotViet 44/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“attributeValue”:string//Giátrịthuộctính
}]//Danhsáchthuộctính
“basePrice”:decimal,//Giábán
“inventory”:[{
“productId”:long,//IDcủasảnphẩm
“productCode”:string,//Mãcủasảnphẩm
“productName”:string,//Têncủasảnphẩm
“branchId”:long,//Idcủachinhánh
"branchName":long,//Têncủachinhánh
"onHand":double?,//Tồnkhotheochinhánh
"cost":decimal?,//Giávốnsảnphẩm
“reserved”:double//Đặthàngtheochinhánh
}]//Danhsáchtồnkhotrêncácchinhánh
“basePrice”:decimal,//Giábántheobảnggiá
“retailerId”:int,//IDcửahàng
“modifiedDate”:datetime,//Thờigiancậpnhật
}
2.4.5. Xóahànghóa
Mụcđíchsửdụng:XóahànghóatheoID
PhươngthứcvàURL:DELETEhttps://public.kiotapi.com/products/{id}
Request:GồmIdcủahànghóatrongURL:
“id”:long//IDcủahànghóa
Response:Trảlạithôngtinxóathànhcông(Code200)
{
"message":"Xóadữliệuthànhcông"
CôngTyCổphầnCôngnghệKiotViet 45/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
2.4.6. Lấythôngtinthuộctínhsảnphẩm
Mụcđíchsửdụng:lấytoànbộthôngtinthuộctínhcủatấtcảcácsảnphẩm
PhươngthứcvàURL:GEThttps://public.kiotapi.com/attributes/allwithdistinctvalue
Response:
[{
“name”:string,//Tênthuộctính
“id”:long,//IDcủathuộctính
“attributeValues”:[
{
“value”:string,//giátrịcủathuộctính
“attributeId”long,//idcủathuộctính
},
{
“value”:string,//giátrịcủathuộctính
“attributeId”long,//idcủathuộctính
},
...
]
}]
2.4.7Thêmmớidanhsáchhànghóa
Mụcđíchsửdụng:Tạomớidanhsáchhànghóa
PhươngthứcvàURL:POSThttps://public.kiotapi.com/listaddproducts
CôngTyCổphầnCôngnghệKiotViet 46/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Request:JSONmãhóayêucầugồm1danhsáchobjecthànghóariêngbiệtvớinhưngtham
sốsau:
{“listProducts”:[{
“name”:string,//Tênhànghóa
“code”:string,//Mãhànghóa
“fullName”:string,//Tênsảnphẩmbaogồmunitvàthuộctính?
“categoryId”:int,//IDnhómhànghóa
“allowsSale”:Boolean,//Sảnphẩmđượcbántrựctiếphaykhông
“description”:string,//Môtảsảnphẩm,
“hasVariants”:boolean,//Sảnphẩmcóthuộctínhhaykhông
“attributes”:[{
“attributeName”:string,//Tênthuộctính(Nếutênthuộctínhchưatồntạitron
ghệthốngthìtựđộngtạomớithuộctính)
“attributeValue”:string//Giátrịthuộctính
}],//Danhsáchthuộctính
“unit”:string,//Đơnvịtínhcủa1sảnphẩm
“masterProductId”:long?,//IDhànghoácùngloại
“masterUnitId”:long,//IDcủahànghóađơnvịcơbản,=NULLnếulàđơnvịcơbản
“conversionValue”:double,
“branchId”:int?,//IDchinhánhhiệntại
“inventories”:[{
"branchId":long,//IDcủachinhánh
"branchName":long,//Têncủachinhánh
"onHand":double?,//Tồnkhotheochinhánh
"cost":decimal?//Giásảnphẩm
}],//Danhsáchtồnkhotrêncácchinhánh
“basePrice”:decimal,//Giásảnphẩm
“weight”:double,//Trọnglượngsảnphẩm
“images”:string[],//Danhsáchhìnhảnhcủahànghóadạnglink
}]
}
Response:
CôngTyCổphầnCôngnghệKiotViet 47/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
{"message":"Thêmmớidanhsáchsảnphẩmthànhcông"}
2.4.8Cậpnhậtdanhsáchhànghóa
Mụcđíchsửdụng:Cậpnhậtdanhsáchhànghóa
PhươngthứcvàURL:PUThttps://public.kiotapi.com/listupdatedproducts
Request:JSONmãhóayêucầugồm1danhsáchobjecthànghóariêngbiệtvớinhưngtham
sốsau:
{“listProducts”:[{
“id”:long,//IDcủahànghóa
“name”:string,//Tênhànghóa
“code”:string,//Mãhànghóa
“fullName”:string,//Tênhànghóabaogồmthuộctínhvàđơnvịtính
“categoryId”:int,//IDnhómhànghóa
"allowsSale":Boolean,//Sảnphẩmđượcbántrựctiếphaykhông
“description”:string,//Môtảsảnphẩm
“hasVariants”:boolean,//Sảnphẩmcóthuộctínhhaykhông
“attributes”:[{
“attributeName”:string,//Tênthuộctính(Nếutênthuộctínhchưatồntạitron
ghệthốngthìtựđộngtạomớithuộctính)
“attributeValue”:string//Giátrịthuộctính
}],//Danhsáchthuộctính
“unit”:string,//Đơnvịtínhcủa1sảnphẩm
“masterProductId”:long?,//IDhànghoácùngloại
“masterUnitId”:long,//IDcủahànghóađơnvịcơbản,=NULLnếulàđơnvịcơbản
“conversionValue”:double,
“branchId”:int?,//IDchinhánhhiệntại
“inventories”:[{
"branchId":long,//IDcủachinhánh
"branchName":long,//Têncủachinhánh
"onHand":double?,//Tồnkhotheochinhánh
"cost":decimal?//Giásảnphẩm
CôngTyCổphầnCôngnghệKiotViet 48/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}],//Danhsáchtồnkhotrêncácchinhánh
“basePrice”:decimal,//Giásảnphẩm
“weight”:double,//Trọnglượngsảnphẩm,
“images”:string[],//DanhsáchhìnhảnhcủahànghóaImage:linkảnhcủahànghóa
}]
}
Response:
{"message":"Cậpnhậtdanhsáchsảnphẩmthànhcông"}
2.4.9Lấydanhsáchtồnkhohànghóa
Mụcđíchsửdụng:Trảvềtoànbộhànghóatheocửahàngđãđượcxácnhận(authenticated
retailer)
PhươngthứcvàURL:GEThttps://public.kiotapi.com/productOnHands
Request:SửdụnghàmGETvớithamsố:
{
“orderBy”:string,optional//SắpxếpdữliệutheotrườngorderBy(vídụ:orderBy=Code)
“lastModifiedFrom”:datetime?,//thờigiancậpnhật
“branchIds”:[int],//danhsáchchinhánh
“pageSize”:int,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int,//lấydữliệutừbảnghicurrentItem
}
Nếucó"OrderDirection",chọnsắpxếpkếtquảvềtheo:
 ASC(Mặcđịnh)
 DESC
Response:
CôngTyCổphầnCôngnghệKiotViet 49/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“total”:int,//Tổngsốhànghóa
“pageSize”:int,
“data”:[{
“id”:long,//IDhànghóa
“code”:string,//Codehànghóa
“createdDate”:datetime?//ngàytạo
"inventories":[
{
“branchId”:long,//Idchinhánh
“onhand”:double,//tồnkho
“reserved”:double,//đặthàngkho
“modifiedDate”:datetime?//ngàycậpnhật
}
],//danhsáchtồnkhocủahànghóatheochinhánh
}],
2.5. Đặthàng
HiệntạiKiotViethỗtrợcácthiếtlậpchotínhnăngđặthàngnhưsau:
 Trong trường hợp người dùng không tích chọn setting cho “Cho phép đặt hàng”, các giao dịch
liên quan tới đặt hàng sẽ không hiển thị trên Kiotviet nữa. Vì vậy, khi gọi các API liên quan tới
phần đặt hàng, nếu thiết lập này đang tắt thì API sẽ trả lại thông báo “Thiết lập “Cho phép đặt
hàng”đangkhôngđượcbật.”.
 Trong trường hợp người dùng không tích chọn setting cho “Sử dụng tính năng giao hàng”, các
giao dịch sẽ không hiển thị tính năng giao hàng nữa. Vì vậy, khi gọi các API liên quan tới phần
giao hàng, nếu thiết lập này đang tắt thì API sẽ trả lại thông báo “Thiết lập “Sử dụng tính năng
giaohàng.” đangkhôngđượcbật”.
 Trong trường hợp người dùng không tích chọn setting cho “Không cho phép thay đổi thời gian
bán hàng”, khi Post/ Put các API liên quan đến thời gian bán hàng thì API sẽ trả lại thông báo
“Thiếtlập“Khôngchophépthayđổithờigianbánhàng”đangkhôngđượcbật.”.
CôngTyCổphầnCôngnghệKiotViet 50/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
MôtảchitiếtchocácAPIhỗtrợĐặthàngnhưsau:
2.5.1. Lấydanhsáchđặthàng
Mụcđíchsửdụng:Trảvềdanhsáchđặthàngtheocửahàngđãđượcxácnhận
PhươngthứcvàURL:GEThttps://public.kiotapi.com/orders
Request:SửdụnghàmGETvớithamsố:
“branchIds”:int[],optional//IDchinhánh
“customerIds”:long[],optional//Idkháchhàng
“customerCode”:string//Mãkháchhàng
“status”:int[],optional//Tìnhtrạngđặthàng
“includePayment”:Boolean,//cólấythôngtinthanhtoán
“includeOrderDelivery”:Boolean,
“lastModifiedFrom”:datetime?//thờigiancậpnhật
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int,
“lastModifiedFrom”:datetime?//thờigiancậpnhật
“toDate”:datetime?//ThờigiancậpnhậtchođếnthờiđiểmtoDate
“orderBy”:string,//SắpxếpdữliệutheotrườngorderBy(Vídụ:orderBy=name)
“orderDirection”:string,//Sắpxếpkếtquảtrảvềtheo:TăngdầnAsc(Mặcđịnh),giảmdần
Desc
“createdDate”:datetime?//Thờigiantạo
“saleChannelId”:int?,optional//Idkênhbánhàng,nếukhôngtruyềnmặcđịnhkênhkhác
Response:
CôngTyCổphầnCôngnghệKiotViet 51/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
{
“total”:int,
“pageSize”:int,
“data”:[{
“id”:long//Idđặthàng
“code”:string//Mãđặthàng
“purchaseDate”:datetime//Ngàyđặthàng
“branchId”:int,//Idchinhánh
“branchName”:string,//Tênchinhánh
“soldById”:long?,
“soldByName”:string
“customerId”:long?,//Idkháchhàng
“customerCode”:string,//Mãkháchhàng
“customerName”:string,//Tênkháchhàng
“total”:decimal,//Kháchcầntrả
“totalPayment”:decimal,//Kháchđãtrả
“discountRatio”:double?,//Giảmgiátrênđơntheo%
“discount”:decimal?,//Giảmgiátrênđơntheotiền
“status”:int,//trạngtháiđơnđặthàng
“statusValue”:string,//trạngtháiđơnđặthàngbằngchữ
“description”:string,//ghichú
“usingCod”:boolean,
“payments”:[{
“id”:long,
“code”:string,
“amount”:decimal,
CôngTyCổphầnCôngnghệKiotViet 52/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“method”:string”,
“status”:byte?,
“statusValue”:string,
“transDate”:datetime,
“bankAccount”:string,
“accountId”:int?
}],
“orderDetails”:{
“productId”:long,//Idhànghóa
“productCode”:string,
“productName”:string,//Tênhànghóabaogồmthuộctínhvàđơnvịtính
“isMaster”:boolean,//Tínhnăngthêmdòng,true:hànghóaởdòngchính,false:
hànghóaởdòngphụ.
“quantity”:double,//Sốlượnghànghóa
“price”:decimal,//Giátrị
“discountRatio”:double?,//Giảmgiátrênsảnphẩmtheo%
“discount”:decimal?,//Giảmgiátrênsảnphẩmtheotiền
“note”:string//Ghichúhànghóa
},
“orderDelivery”:{
“deliveryCode”:string,
“type”:byte?,
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
“address”:string,
CôngTyCổphầnCôngnghệKiotViet 53/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“locationId”:int?,
“locationName”:string,
“weight”:double?,
“length”:double?,
“width”:double?,
“height”:double?,
“partnerDeliveryId”:long?,
“partnerDelivery”:{
“code”:string,
“name”:string,
“address”:string,
“contactNumber”:string,
“email”:string
}
}
“retailerId”:int,//Idcửahàng
“modifiedDate”:datetime//thờigiancậpnhật
“createdDate”:datetime//thờigiantạo
“saleChannelId”:int?,optional//Idkênhbánhàng,nếukhôngtruyềnmặcđịnhkênhkhác
}]
}
2.5.2. Lấychitiếtđặthàng
Mụcđíchsửdụng:TrảvềthôngtinchitiếtcủađơnđặthàngtheoID,theoCode
PhươngthứcvàURL:
- TheoId: GEThttps://public.kiotapi.com/orders/{id}
- TheoCode: GEThttps://public.kiotapi.com/orders/code/{code}
CôngTyCổphầnCôngnghệKiotViet 54/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Request:SửdụnghàmGETvớithamsố:
“id”:long//IDcủađơnđặthàng
“code”:code//Mãcủađơnđặthàng
Response:
{
“id”:long//Idđặthàng
“code”:string//Mãđặthàng
“purchaseDate”:datetime//Ngàyđặthàng
“branchId”:int,//Idchinhánh
“branchName”:string,//Tênchinhánh
“soldById”:long?,
“soldByName”:string
“customerId”:long?,//Idkháchhàng
“customerName”:string,//Tênkháchhàng
“total”:decimal,//Kháchcầntrả
“totalPayment”:decimal,//Kháchđãtrả
“discountRatio”:double,//Giảmgiátrênđơntheo%
“discount”:decimal?,//Giảmgiátrênđơntheotiền
“status”:int,//trạngtháiđơnđặthàng
“statusValue”:string,//trạngtháiđơnđặthàngbằngchữ
“description”:string,//ghichú
“usingCod”:boolean,
“payments”:[{
“id”:long,
“code”:string,
“amount”:decimal,
CôngTyCổphầnCôngnghệKiotViet 55/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“method”:string,
“status”:byte?,
“statusValue”:string,
“transDate”:datetime,
“bankAccount”:string,
“accountId”:int?
}],
“orderDetails”:{
“productId”:long,//Idhànghóa
“productCode”:string,
“productName”:string,//Tênhànghóabaogồmthuộctínhvàđơnvịtính
“quantity”:double,//Sốlượnghànghóa
“isMaster”:boolean,//Tínhnăngthêmdòng,true:hànghóaởdòngchính,false:
hànghóaởdòngphụ.
“price”:decimal,//Giátrị
“discountRatio”:double?,//Giảmgiátrênsảnphẩmtheo%
“discount”:decimal?,//Giảmgiátrênsảnphẩmtheotiền
“note”:string//Ghichúhànghóa
},
“orderDelivery”:{
“deliveryCode”:string,
“type”:byte?,
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
“address”:string,
CôngTyCổphầnCôngnghệKiotViet 56/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“locationId”:int?,
“locationName”:string,
“weight”:double?,
“length”:double?,
“width”:double?,
“height”:double?,
“partnerDeliveryId”:long?,
“partnerDelivery”:{
“code”:string,
“name”:string,
“address”:string,
“contactNumber”:string,
“email”:string
}
},
"invoiceOrderSurcharges":[{
"id":long,
"invoiceId":long?,
"surchargeId":long?,
"surchargeName":string,
"surValue":decimal?,
"price":decimal?,
"createdDate":DateTime
}],
“retailerId”:int,//Idcửahàng
“modifiedDate”:datetime,//thờigiancậpnhật
CôngTyCổphầnCôngnghệKiotViet 57/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“createdDate”:datetime//thờigiantạo
}
2.5.3. Thêmmớiđặthàng
Mụcđíchsửdụng:Tạomớiđơnđặthàng
PhươngthứcvàURL:POSThttps://public.kiotapi.com/orders
Request:JSONmãhóayêucầugồm1objectđặthàng:
Chú ý: Khi thêm mới đơn đặt hàng từ MyKiot hoặc KV Sync sẽ thêm param Partner vào
header:
 TừMyKiot:
Partner:MyKiot
o
 TừKVSync:
Partner:KVSync
o
{
“isApplyVoucher”:true,//Cóapplyvoucherkhitạođặthàngkhông
“purchaseDate”:datetime,
“branchId”:int,
“soldById”:long?,
“cashierId”:long?,//IDngườitạođơnđặthàng,nếukhôngtruyềnthìmặcđịnhAdminlà
ngườitạo
“discount”:decimal,
“description”:string,
“method”:string,
“totalPayment”:decimal,//Kháchđãtrả
“accountId”:int?,//Idaccounttàikhoảnngânhàngnếuphươngthứcthanhtoánlà
TRANSFER,CARD,
CôngTyCổphầnCôngnghệKiotViet 58/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“makeInvoice”:bool,//Tạohóađơntừđơnđặthàng,tạophiếuthuchohóađơnđóvới
thờiđiểmhiệntại,
“saleChannelId”:int?,optional//Idkênhbánhàng,nếukhôngtruyềnmặcđịnhkênhkhác
“orderDetails”:[{
“productId”:long,
“productCode”:string,
“productName”:string,
“isMaster”:boolean,//Tínhnăngthêmdòng,true:hànghóaởdòngchính,false:
hànghóaởdòngphụ.
“quantity”:double,
“price”:decimal,
“discount”:decimal?,
“discountRatio”:double?,
“note”:string
}],
“orderDelivery”:{
“deliveryCode”:string,
“type”:byte?,
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
“address”:string,
“locationId”:int?,
“locationName”:string,
“wardName”:string,//Tênphường
“weight”:double,
CôngTyCổphầnCôngnghệKiotViet 59/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“length”:double,
“width”:double,
“height”:double,
“partnerDeliveryId”:long?,
“expectedDelivery”:datetime,
“partnerDelivery”:{
“code”:string,
“name”:string,
“address”:string,
“contactNumber”:string,
“email”:string
}
},
“customer":{
“id”:long,
"code":string,
"name":string,
"gender":boolean,
"birthDate":datetime,
"contactNumber":string,
"address":string,
“wardName”:string,//Tênphường
"email":string,
"comments":string
},
“surchages”:[{
CôngTyCổphầnCôngnghệKiotViet 60/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“id”:int,
“code”:string,
“price”:decimal,
}],
“Payments”:[{//Thêmphươngthứcthanhtoánbằngvoucher
"Method":"Voucher",//GiátrịmặcđịnhlàVoucher(khôngđổi)
"MethodStr":"Voucher",//GiátrịmặcđịnhlàVoucher(khôngđổi)
"Amount":50000,//Giátrịcủavoucher
"Id":-1,//Giátrịmặcđịnhlà-1(khôngđổi)
"AccountId":null,//Giátrịmặcđịnhlànull(khôngđổi)
"VoucherId":30996,//Idcủavoucher
"VoucherCampaignId":30087//Idcủađợtpháthànhvoucher
}]
}
Response:
{
“id”:long,
“code”:string,
“purchaseDate”:datetime,
“branchId”:int,
“branchName”:string,
“soldById”:long?,
“soldByName”:string,
“customerId”:long?,
“customerName”:string,
CôngTyCổphầnCôngnghệKiotViet 61/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“total”:decimal,//Kháchcầntrả
“totalPayment”:decimal,//Kháchđãtrả
“discountRatio”:double?,//Giảmgiátrênđơntheo%
“discount”:decimal?,//Giảmgiátrênđơntheotiền
“method”:string,//Phươngthứcthanhtoán(Cash,Card,Transfer)
“status”:int,//trạngtháiđơnđặthàng
“statusValue”:string,//trạngtháiđơnđặthàngbằngchữ
“description”:string,//ghichú
"usingCod":boolean,
“saleChannelId”:int?,optional//Idkênhbánhàng,nếukhôngtruyềnmặcđịnhkênhkhác
“orderDetails”:{
“productId”:long,//Idhànghóa
“productName”:string,//Tênhànghóabaogồmthuộctínhvàđơnvịtính
“isMaster”:Boolean,
“quantity”:double,//Sốlượnghànghóa
“price”:decimal,//Giátrị
“discountRatio”:double?,//Giảmgiátrênsảnphẩmtheo%
“discount”:decimal?,//Giảmgiátrênsảnphẩmtheotiền
“note”:string//Ghichúhànghóa
},
“orderDelivery”:{
“deliveryCode”:string,
“type”:byte?,
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
CôngTyCổphầnCôngnghệKiotViet 62/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“address”:string,
“locationId”:int?,
“locationName”:string,
“wardName”:string,//Tênphường
“weight”:double?,
“length”:double?,
“width”:double?,
“height”:double?,
“partnerDeliveryId”:long?,
“partnerDelivery”:{
“code”:string,
“name”:string,
“address”:string,
“contactNumber”:string,
“email”:string
}
}
“payments”:[{
“id”:long,
“code”:string,
“amount”:decimal,
“method”:string,
“status”:byte?,
“statusValue”:string,
“transDate”:datetime,
“bankAccount”:string,
CôngTyCổphầnCôngnghệKiotViet 63/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“accountId”:int?
}],
"invoiceOrderSurcharges":[{
"id":long,
"invoiceId":long?,
"surchargeId":long?,
"surchargeName":string,
"surValue":decimal?,
"price":decimal?,
"createdDate":DateTime
}]
}
2.5.4. Cậpnhậtđặthàng
Mụcđíchsửdụng:CậpnhậtđơnđặthàngtheoID
PhươngthứcvàURL:PUThttps://public.kiotapi.com/orders/Id
Request:SửdụnghàmPUTvớiIDđơnđặthàngqua1objectJSON.
“id”:long//IDđơnđặthàng
Body
{
“purchaseDate”:datetime,
“branchId”:int,
“soldById”:long?,
“cashierId”:long?,//IDngườitạođơnđặthàng,nếukhôngtruyềnthìmặcđịnhAdminlà
ngườitạo
“discount”:decimal,
“description”:string,
CôngTyCổphầnCôngnghệKiotViet 64/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“method”:string,
“totalPayment”:decimal,//Kháchđãtrả,
“accountId”:int?,//Idaccounttàikhoảnngânhàngnếuphươngthứcthanhtoánlà
TRANSFER,CARD,
“makeInvoice”:bool,//Tạohóađơntừđơnđặthàng,tạophiếuthuchohóađơnđóvới
thờiđiểmhiệntại,
“saleChannelId”:int?,optional//Idkênhbánhàng,nếukhôngtruyềnmặcđịnhkênhkhác
“orderDetails”:[{
“productId”:long,
“productCode”:string,
“productName”:string,
“isMaster”:boolean,//Tínhnăngthêmdòng,true:hànghóaởdòngchính,false:
hànghóaởdòngphụ.
“quantity”:double,
“price”:decimal,
“discount”:decimal?,
“discountRatio”:double?
}]
“orderDelivery”:{
“deliveryCode”:string,
“type”:byte?,
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
“address”:string,
“locationId”:int?,
“locationName”:string,
CôngTyCổphầnCôngnghệKiotViet 65/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“wardName”:string,//Tênphường
“weight”:double?,
“length”:double?,
“width”:double?,
“height”:double?,
“partnerDeliveryId”:long?,
“expectedDelivery”:datetime,
“partnerDelivery”:{
“code”:string,
“name”:string,
“address”:string,
“contactNumber”:string,
“email”:string
}
},
“customer":{
“id”:long,
"code":string,
"name":string,
"gender":boolean,
"birthDate":datetime,
"contactNumber":string,
"address":string,
“wardName”:string,//Tênphường
"email":string,
"comments":string
CôngTyCổphầnCôngnghệKiotViet 66/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
},
“surchages”:[{
“id”:int,
“code”:string,
“price”:decimal,
}]
}
Response:
{
“id”:long,
“code”:string,
“purchaseDate”:datetime,
“branchId”:int,
“branchName”:string,
“soldById”:long?,
“soldByName”:string,
“customerId”:long,
“customerName”:string,
“total”:decimal,//Kháchcầntrả
“totalPayment”:decimal,//Kháchđãtrả
“discountRatio”:double?,//Giảmgiátrênđơntheo%
“discount”:decimal?,//Giảmgiátrênđơntheotiền
“method”:string,//Phươngthứcthanhtoán(Cash,Card,Transfer)
CôngTyCổphầnCôngnghệKiotViet 67/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“status”:int,//trạngtháiđơnđặthàng
“statusValue”:string,//trạngtháiđơnđặthàngbằngchữ
“description”:string,//ghichú
"usingCod":boolean,
“saleChannelId”:int?,optional//Idkênhbánhàng,nếukhôngtruyềnmặcđịnhkênhkhác
“orderDetails”:{
“productId”:long,//Idhànghóa
“productName”:string,//Tênhànghóabaogồmthuộctínhvàđơnvịtính
“isMaster”:Boolean,
“quantity”:double,//Sốlượnghànghóa
“price”:decimal,//Giátrị
“discountRatio”:double?,//Giảmgiátrênsảnphẩmtheo%
“discount”:decimal?,//Giảmgiátrênsảnphẩmtheotiền
},
“orderDelivery”:{
“deliveryCode”:string,
“type”:byte?,
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
“address”:string,
“locationId”:int?,
“locationName”:string,
“wardName”:string,//Tênphường
“weight”:double?,
“length”:double?,
CôngTyCổphầnCôngnghệKiotViet 68/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“width”:double?,
“height”:double?,
“partnerDeliveryId”:long?,
“partnerDelivery”:{
“code”:string,
“name”:string,
“address”:string,
“contactNumber”:string,
“email”:string
}
},
“payments”:[{
“id”:long,
“code”:string,
“amount”:decimal,
“method”:string”,
“status”:byte?,
“statusValue”:string,
“transDate”:datetime,
“bankAccount”:string,
“accountId”:int?
}],
"invoiceOrderSurcharges":[{
"id":long,
"invoiceId":long?,
"surchargeId":long?,
CôngTyCổphầnCôngnghệKiotViet 69/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"surchargeName":string,
"surValue":decimal?,
"price":decimal?,
"createdDate":DateTime
}]
}
2.5.5. Xóađặthàng
Mụcđíchsửdụng:XóađơnđặthàngtheoID
PhươngthứcvàURL:DELETEhttps://public.kiotapi.com/orders/{id}?IsVoidPayment=true
Request:GồmIdcủađơnđặthàngtrongURL:
“id”:long//IDcủađơnđặthàng
“IsVoidPayment”:bool//Hủyphiếuthanhtoán,nếukhôngtruyềnthamsốnàythìmặcđịnhkhông
hủyphiếuthanhtoángắnkèmđặthàng
Response:Trảlạithôngtinxóathànhcông(Code200)
{
"message":"Xóadữliệuthànhcông"
}
2.6. Kháchhàng
Môtảchitiếtchocácliênquanđếnthôngtinhànghóanhưsau:
2.6.1. Lấydanhsáchkháchhàng
Mụcđíchsửdụng:Trảlạidanhsáchkháchhàngtheocửahàngđãđượcxácnhận
PhươngthứcvàURL:GEThttps://public.kiotapi.com/customers
CôngTyCổphầnCôngnghệKiotViet 70/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Request:SửdụnghàmGETvớithamsố:
“code”: string, optional // nếu có mã code, cho phép tìm kiếm khách hàng theo mã
KH
“name”:string,optional//tìmkiếmtheotênkháchhàng
“contactNumber”:string,optional//tìmkiếmtheosốđiệnthoạikháchhàng
“lastModifiedFrom”:datetime?//thờigiancậpnhật
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int?,
“orderBy”:string,//SắpxếpdữliệutheotrườngorderBy(Vídụ:orderBy=name)
“orderDirection”: string, //Sắp xếp kết quả trả về theo: Tăng dần Asc (Mặc định),
giảmdầnDesc
“includeRemoveIds”: boolean, //Có lấy thông tin danh sách Id bị xoá dựa trên
lastModifiedFrom
“includeTotal”:boolean,//CólấythôngtinTotalInvoice,TotalPoint,TotalRevenue
“includeCustomerGroup”:boolean,//Cólấythôngtinnhómkháchhànghaykhông
“birthDate”:string//filterkháchhàngtheongàysinhnhật
“groupId”:int,//filtertheonhómkháchhàng
“includeCustomerSocial”: boolean, // Có lấy thông tin Psid facebook fanpage của khách
hànghaykhông
Response:
{
“total”:int,
“pageSize”:int,
“data”:[
{
“id”:long,//IDkháchhàng
"code":string,//Mãkháchhàng
CôngTyCổphầnCôngnghệKiotViet 71/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"name":string,//Tênkháchhàng
“gender”:Boolean?,//Giớitính(true:nam,false:nữ)
"birthDate":date?,//Ngàysinhkháchhàng
"contactNumber":string,//Sốđiệnthoạikháchhàng
“address”:string,//Địachỉkháchhàng
“locationName”:string,//Khuvực
“wardName”:string,//Phườngxã
"email":string,//Emailcủakháchhàng
"organization":string,//Côngty
"comments":string,//Ghichú
"taxCode":string,//Mãsốthuế
"debt":decimal,//Nợhiệntại
"totalInvoiced":decimal?,//Tổngbán
"totalPoint":double?,//Tổngđiểm
"totalRevenue":decimal?,
“retailerId”:int,//Idcửahàng
“modifiedDate”:datetime?//thờigiancậpnhật
“createdDate”:datetime,
“rewardPoint”:long?//Điểmhiệntại
“psidFacebook”:long?//Psidfacebookfanpage
}],
“removeId”:int[]//danhsáchIdkháchhàngbịxóadựatrênModifiedDate
}
2.6.2. Lấychitiếtkháchhàng
Mụcđíchsửdụng:TrảlạithôngtinchitiếtcủakháchhàngtheoID,theoCode
CôngTyCổphầnCôngnghệKiotViet 72/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
PhươngthứcvàURL:
- TheoId:GEThttps://public.kiotapi.com/customers/{id}
- TheoCode:GEThttps://public.kiotapi.com/customers/code/{code}
Request:SửdụnghàmGETvớithamsố:
“id”:long//IDcủakháchhàng
“code”:string//Mãcủakháchhàng
Response:
{
“id”:long,//IDkháchhàng
"code":string,//Mãkháchhàng
"name":string,//Tênkháchhàng
“gender”:Boolean?,//Giớitính(true:nam,false:nữ)
"birthDate":datetime?,//Ngàysinhkháchhàng
"contactNumber":string,//Sốđiệnthoạikháchhàng
“address”:string,//Địachỉkháchhàng
“locationName”:string,//Khuvực
“wardName”:string,//Phườngxã
"email":string,//Emailcủakháchhàng
"organization":string,//Côngty
"comments":string,//Ghichú
"taxCode":string,//Mãsốthuế
“retailerId”:int,//Idcửahàng
"debt":decimal,//Nợhiệntại
"totalInvoiced":decimal?,//Tổngbán
"totalPoint":double?,//Tổngđiểm
"totalRevenue":decimal?,
CôngTyCổphầnCôngnghệKiotViet 73/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“modifiedDate”:datetime?//thờigiancậpnhật
“createdDate”:datetime
“groups”:string//danhsáchtênnhómkháchhàng,
“rewardPoint”:long?//Điểmhiệntại
“psidFacebook”:long?//Psidfacebookfanpage
}
2.6.3. Thêmmớikháchhàng
Mụcđíchsửdụng:Tạomớikháchhàng
PhươngthứcvàURL:POSThttps://public.kiotapi.com/customers
Request:JSONmãhóayêucầugồm1objectkháchhàng:
{
“code”:string,//Makhachhang
“name”:string,//Tênkháchhàng
“gender”:Boolean,//Giớitính(true:nam,false:nữ)
"birthDate":datetime?,//Ngàysinhkháchhàng
"contactNumber":string,//Sốđiệnthoạikháchhàng
"address":string,//Địachỉkháchhang
“locationName”:string,//Khuvực
“wardName”:string,//Phườngxã
"email":string,//Emailcủakháchhàng
"comments":string,//Ghichú
"groupIds":int[]//DanhsáchIdnhómkháchhàng
“branchId”:int[]//IDchinhánhtạokháchhàng
}
CôngTyCổphầnCôngnghệKiotViet 74/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Response:
{
“id”:long,//IDkháchhàng(vớiid=-1làbảnghiđầutiênchứa thôngtintổngquan)
"code":string,//Mãkháchhàng
"name":string,//Tênkháchhàng
"type":int,//Loạikháchhàng(0:Cánhân,1:Côngty)
“gender”:Boolean,//Giớitính(true:nam,false:nữ)
"birthDate":datetime?,//Ngàysinhkháchhàng
"contactNumber":string,//Sốđiệnthoạikháchhàng
“address”:string,//Địachỉkháchhàng
“locationName”:string,//Khuvực
"email":string,//Emailcủakháchhàng
"organization":string,//Têncôngtycủakháchhàng(nếulàkhách hàngcôngty)
"comments":string,//Ghichú
"taxCode":string,//Mãsốthuế
“retailerId”:int,//Idcửahàng
“modifiedDate”:datetime?,//Thờigiancậpnhật
“createdDate”:datetime
"customerGroupDetails":[
{
"id":long//IdChitiếtnhómkháchhàng
"customerId":long//Idkháchhàng
"groupId":int//Idnhómkháchhàng
}
],
CôngTyCổphầnCôngnghệKiotViet 75/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
2.6.4. Cậpnhậtkháchhàng
Mụcđíchsửdụng:CậpnhậtthôngtinkháchhàngtheoID
PhươngthứcvàURL:PUThttps://public.kiotapi.com/customers/Id
Request:SửdụnghàmPUTvớiIDkháchhàngqua1objectJSON.
“id”:long//IDkháchhàng
Body
{
“code”:string,//Mãkháchhàng
“name”:string,//Tênkháchhàng
“gender”:Boolean,//Giớitính(true:nam,false:nữ)
"birthDate":datetime?,//Ngàysinhkháchhàng
"contactNumber":string,//Sốđiệnthoạikháchhàng
"address":string,//Địachỉkháchhang
“locationName”:string,//Khuvực
“wardName”:string,//Phườngxã
"email":string,//Emailcủakháchhàng
"comments":string,//Ghichú
"groupIds":int[]//DanhsáchIdnhómkháchhàng
“taxCode”:string//Mãsốthuế
}
Response:
{
“id”:long,//IDkháchhàng(vớiid=-1làbảnghiđầutiênchứathôngtintổngquan)
CôngTyCổphầnCôngnghệKiotViet 76/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"code":string,//Mãkháchhàng
"name":string,//Tênkháchhàng
“gender”:Boolean,//Giớitính(true:nam,false:nữ)
"birthDate":datetime?,//Ngàysinhkháchhàng
"contactNumber":string,//Sốđiệnthoạikháchhàng
“address”:string,//Địachỉkháchhàng
“locationName”:string,//Khuvực
"email":string,//Emailcủakháchhàng
"organization":string,//Têncôngtycủakháchhàng(nếulàkhách hàngcôngty)
"comments":string,//Ghichú
"taxCode":string,//Mãsốthuế
“retailerId”:int,//Idcửahàng
“modifiedDate”:datetime?,//Thờigiancậpnhật
“createdDate”:datetime,
“groups”:string,//danhsáchtênnhóm
}
2.6.5. Xóakháchhàng
Mụcđíchsửdụng:XóakháchhàngtheoID
PhươngthứcvàURL:DELETEhttps://public.kiotapi.com/customers/{id}
Request:GồmIdcủakháchhàngtrongURL:
“id”:long//IDcủakháchhàng
Response:Trảlạithôngtinxóathànhcông(Code200)
{
"message":"Xóadữliệuthànhcông"
CôngTyCổphầnCôngnghệKiotViet 77/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
2.6.6Thêmmớidanhsáchkháchhàng
Mụcđíchsửdụng:Thêmmớidanhsáchkháchhàng
PhươngthứcvàURL:POSThttps://public.kiotapi.com/listaddcutomers
Request: JSON mã hóa yêu cầu gồm 1 danh sách object khách hàng riêng biệt với nhưng
thamsốsau:
{“listCustomers”:[
{
“code”:string,//Makhachhang
“name”:string,//Tênkháchhàng
“gender”:Boolean,//Giớitính(true:nam,false:nữ)
"birthDate":datetime?,//Ngàysinhkháchhàng
"contactNumber":string,//Sốđiệnthoạikháchhàng
"address":string,//Địachỉkháchhang
“locationName”:string,//Khuvực
“wardName”:string,//Phườngxã
"email":string,//Emailcủakháchhàng
"comments":string,//Ghichú
},
…]
}
Response:
{
"message":"Thêmmớidanhsáchkháchhàngthànhcông"
}
2.6.7Cậpnhậtdanhsáchkháchhàng
Mụcđíchsửdụng:Cậpnhậtdanhsáchkháchhàng
CôngTyCổphầnCôngnghệKiotViet 78/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
PhươngthứcvàURL:PUThttps://public.kiotapi.com/listupdatecustomers
Request: JSON mã hóa yêu cầu gồm 1 danh sách object khách hàng riêng biệt với nhưng
thamsốsau:
{“listCustomers”:[//danhsáchkháchhàng
{
“id”:long,//Idkháchhàng
“code”:string,//Makhachhang
“name”:string,//Tênkháchhàng
“gender”:Boolean,//Giớitính(true:nam,false:nữ)
"birthDate":datetime?,//Ngàysinhkháchhàng
"contactNumber":string,//Sốđiệnthoạikháchhàng
"address":string,//Địachỉkháchhang
“locationName”:string,//Khuvực
“wardName”:string,//Phườngxã
"email":string,//Emailcủakháchhàng
"comments":string,//Ghichú
},
…]
}
Response:
{
"message":"Cậpnhậtdanhsáchkháchhàngthànhcông"
}
2.7. Lấydanhsáchchinhánh
Mụcđíchsửdụng:Trảlạidanhsáchtoànbộchinhánhcủacửahàngđãđượcxácnhận
PhươngthứcvàURL:GEThttps://public.kiotapi.com/branches
Request:SửdụnghàmGETvớithamsố:
“lastModifiedFrom”:datetime?//thờigiancậpnhật
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int?,
“orderBy”:string,//SắpxếpdữliệutheotrườngorderBy(Vídụ:orderBy=name)
CôngTyCổphầnCôngnghệKiotViet 79/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“orderDirection”: string, //Sắp xếp kết quả trả về theo: Tăng dần Asc (Mặc định),
giảmdầnDesc,
“includeRemoveIds”: boolean, //Có lấy thông tin danh sách Id bị xoá dựa trên
lastModifiedFrom
Response:
{
"removedIds":int[],//chinhánhngừnghoạtđộng
"total":int,
“pageSize”:int,
"data":[
{
"id":int,//Idchinhánh
"branchName":string,
“branchCode”:string,
"contactNumber":string,
"retailerId":int,//Idcửahàng
"email":string,
“address”:string,
"modifiedDate":datetime?
“createdDate”:datetime
}
],
"timestamp":datetime
}
CôngTyCổphầnCôngnghệKiotViet 80/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
2.8. Lấydanhsáchngườidùng
Mụcđíchsửdụng:Trảlạidanhsáchtoànbộngườidùngcủacửahàngđãđượcxácnhậnvà
khôngchothấythôngtinSuperAdmin(isAdmin=true).
PhươngthứcvàURL:GEThttps://public.kiotapi.com/users
Request:SửdụnghàmGETvớithamsố:
“lastModifiedFrom”:datetime?//thờigiancậpnhật
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int?,
“orderBy”:string,//SắpxếpdữliệutheotrườngorderBy(Vídụ:orderBy=name)
“orderDirection”: string, //Sắp xếp kết quả trả về theo: Tăng dần Asc (Mặc định),
giảmdầnDesc,
“includeRemoveIds”: boolean //Có lấy thông tin danh sách Id bị xoá dựa trên
lastModifiedFrom
Response:
{
“total”:int,
“pageSize”:int,
“data”:[
{
“id”:long,//IDngườidùng
"userName":string,//Tênđăngnhập
"givenName":string,//Họtên
“address”:string,//Địachỉ
“mobilePhone”:string//Điệnthoại
“email”:string,//Email
“description”:string,//ghichú
“retailerId”:int,//Idcửahàng
CôngTyCổphầnCôngnghệKiotViet 81/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“birthDate”:date//Ngàysinh
“createdDate”:datetime
}],
“removeIds”: int [] // danh sách khách hàng bị xóa và ngừng hoạt động dựa trên
ModifiedDate
}
2.9. Lấydanhsáchtàikhoảnngânhàng
Mụcđíchsửdụng:Trảlạitoànbộdanhsáchtàikhoảnngânhàngcủacửahàngđãđượcxác
nhận
PhươngthứcvàURL:GEThttps://public.kiotapi.com/BankAccounts
Request:SửdụnghàmGETvớithamsố
“lastModifiedFrom”:datetime?//thờigiancậpnhật
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int?,
“orderBy”:string,//SắpxếpdữliệutheotrườngorderBy(Vídụ:orderBy=name)
“orderDirection”: string, //Sắp xếp kết quả trả về theo: Tăng dần Asc (Mặc định),
giảmdầnDesc,
“includeRemoveIds”: boolean, //Có lấy thông tin danh sách Id bị xoá dựa trên
lastModifiedFrom
Response:
{
“total”:int,
“pageSize”:int,
“data”:[
{
CôngTyCổphầnCôngnghệKiotViet 82/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“id”:int,//IDtàikhoảnngânhàng
"bankName":string,//Têntàikhoảnngânhàng
"accountNumber":string,//Sốtàikhoảnngânhàng
“description”:string,//ghichú
“retailerId”:int,//Idcửahàng
“modifiedDate”:datetime?//thờigiancậpnhật,
“createdDate”:datatime
}],
“removeIds”:int[]//danhsáchkháchhàngbịxóadựatrênModifiedDate
}
2.10. Thukhác
2.10.1. Lấydanhsáchthukhác
Mụcđíchsửdụng:Trảlạitoànbộdanhsáchthukháccủacửahàngđãđượcxácnhận
PhươngthứcvàURL:GEThttps://public.kiotapi.com/surchages
Request:SửdụnghàmGETvớithamsố:
“branchId”:int?,//Idchinhánh
“lastModifiedFrom”:datetime?//thờigiancậpnhật
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int?,
“orderBy”:string,//SắpxếpdữliệutheotrườngorderBy(Vídụ:orderBy=name)
“orderDirection”: string, //Sắp xếp kết quả trả về theo: Tăng dần Asc (Mặc định),
giảmdầnDesc,
Response:
{
CôngTyCổphầnCôngnghệKiotViet 83/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“total”:int,
“pageSize”:int,
“data”:[
{
“id”:long,//Idthukhác
"surchargeCode":string,//Mãthukhác
"surchargeName":string,//Tênthukhác
“valueRatio”:double,//Phầntrămthukhác
“value”:decimal?//Giátrịthukhác
“retailerId”:int,//Idcửahàng
“modifiedDate”:datetime?//thờigiancậpnhật
“createDate”:datetime
}]
}
Chúý:HiệntạiKiotViethỗtrợcácthiếtlậpchotínhnăngthukhácnhưsau:
Trong trường hợp người dùng không tích chọn setting cho “Hỗ trợ các khoản thu khác khi
bánhàng”,khigọicácAPIdanhsáchthukhác,APIsẽtrảlạithôngbáoexception“Chưabậtthukhác
trongthiếtlậpcửahàng”.
2.10.2. Thêmmớithukhác
Mụcđíchsửdụng:Thêmmớimộtthukhác
PhươngthứcvàURL:POSThttps://public.kiotapi.com/surchages
Request:JSONmãhóayêucầugồm1objectnhómhàngriêngbiệtvớinhưngthamsốsau:
{
“name”:string//tênthukhác
“code”:string//mãthukhác(nếukhôngtruyềnlên,hệthốngsẽtựđộngsinhmãcode)
“value”:decimal?//giátrịthukhác
CôngTyCổphầnCôngnghệKiotViet 84/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
Response:
{
"message":"Thôngtinthukhácđượccậpnhậtthànhcông",
"data":{
“id”:long,//Idthukhác
"surchargeCode":string,//Mãthukhác
"surchargeName":string,//Tênthukhác
“valueRatio”:double,//Phầntrămthukhác
“value”:decimal?//Giátrịthukhác
“retailerId”:int,//Idcửahàng
“modifiedDate”:datetime?//thờigiancậpnhật
“createDate”:datetime
}
}
2.10.3. Cậpnhậtthukhác
Mụcđíchsửdụng:Cậpnhậtmộtthukhác
PhươngthứcvàURL:PUThttps://public.kiotapi.com/surchages/id
Request:JSONmãhóayêucầugồm1objectnhómhàngriêngbiệtvớinhưngthamsốsau:
“id”:long//IDthukhác
Body
{
“name”:string//tênthukhác
“code”:string//mãthukhác
CôngTyCổphầnCôngnghệKiotViet 85/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“value”:decimal//giátrịthukhác
}
Response:
{
"message":"Thôngtinthukhácđượccậpnhậtthànhcông",
"data":{
“id”:long,//Idthukhác
"surchargeCode":string,//Mãthukhác
"surchargeName":string,//Tênthukhác
“valueRatio”:double,//Phầntrămthukhác
“value”:decimal?//Giátrịthukhác
“retailerId”:int,//Idcửahàng
“modifiedDate”:datetime?//thờigiancậpnhật
“createDate”:datetime
}
2.10.4. Ngừnghoạtđộngthukhác
Mụcđíchsửdụng:Ngừng/chophéphoạtđộng1thukhác
PhươngthứcvàURL:POSThttps://public.kiotapi.com/surchages/id/activesurchage
Request:JSONmãhóayêucầugồm1objectnhómhàngriêngbiệtvớinhưngthamsốsau:
“id”:long//IDthukhác
Body
{
“isActive”:bool//true:chophephoạtđộng;false:ngừnghoạtđộng
}
CôngTyCổphầnCôngnghệKiotViet 86/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Response:
{
"message":"Cậpnhậtdữliệuthànhcông",
}
2.11. Webhook
WebhooklàmôhìnhmộtpublicAPIchủđộnggọivàomộtservercủabênthứbakhicóthay
đổixảyra.Nótươngđươngvớimôhìnhdatapush(tráingượcvớipolling),trongđóserverchủđộng
gọichoclientthayvìclientphảithườngxuyênkiểmtraserver.
Lưuý:
-Thờigianphảnhồicủamộtyêucầutốiđalà5giây,vượtquangưỡngnàythìyêucầugửitừ
KiotVietsangbênđăngkýđượctínhlàthấtbại.
- Bên đăng ký hãy lựa chọn mã trạng thái và cách xử lý với yêu cầu không hợp lệ một cách
hợp lý, vì trong trường hợp yêu cầu gửi đến là từ phía KiotViet, và mã trạng thái phản hồi của bên
đăng ký thuộc một trong số các mã 4xx (400, 401, 403, 404, 405) thì dịch vụ webhook KiotViet sẽ
ngưngviệcgửiyêucầutớiđiểmcuốiđó.
APIWebhookđượcmôtảchitiếtnhưbêndưới:
2.11.1. ĐăngkýWebhook
Mụcđíchsửdụng:Đăngkýwebhook
PhươngthứcvàURL:POSThttps://public.kiotapi.com/webhooks
Request:
{
"Webhook":{
“Type”:string,//Kiểusựkiện
“Url”:string,//Địachỉđăngký(điểmcuối)
“IsActive”:boolean,//Trạngtháihoạtđộng
"Description":string, //Môtả
“Secret”:string //Mãbímật(khôngbắtbuộcsửdụng)
CôngTyCổphầnCôngnghệKiotViet 87/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
}
Response:
{
“id”:long,//webhookid
“type”:string,//Kiểusựkiện
“url”:string,//Địachỉđăngký (điểmcuối)
“isActive”:boolean,//Trạngtháihoạtđộng
"retailerId":int,//Idcửahàng
"description":string,//Môtả
}
Thôngtinvềmãbímật:
KiotViettriểnkhaiviệc tạochữkýsửdụngthuật toánHMACSHA-256từmãbímật (secret)
dobênđăngkýcungcấpkhitạowebhookkếthợpvớidữliệu(requestbody)từyêucầugửitớibên
đăngký.
varhash=body.CreateHmacSignature(Secret);//UsingHMACSHA-256algorithm
httpRequest.Headers.Add("X-Hub-Signature",hash);
Lưuý:
-Cơchếtạochữkýnàychỉgiúpđảmbảo“tínhxácthực”(authenticity)rằngdữliệuthựcsự
đượcgửitừnguồnmàbênđăngkýmongmuốn(ởđâychínhlàdịchvụwebhookcủaKiotViet).
- Cơ chế này không mã hoá dữ liệu (payload) hoặc cung cấp thêm bất kỳ tính bảo mật nào
khác(confidentiality),dữliệuvẫnsẽởdạngvănbảnthuầntúy(plaintext),dođóđiểmcuốicủabên
đăngkýphảiluônđượcbảomậtbằngHTTPS.
Cáchsửdụngmãbímật:
1.Tạo vàlưutrữ mãbí mật ngẫu nhiên(8 ký tự trở lên), sau đómãhoácác ký tự này bằng
Base64
CôngTyCổphầnCôngnghệKiotViet 88/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
2.Khiđăngkýwebhook,trongyêucầuđăngkýtruyềnthêmSecretvớigiátrịlàmãbímậtđã
đượcmãhoáởbước1:
{
"Webhook":{
"Type":String,//Kiểusựkiện
"Url":String,//Địachỉđăngký(điểmcuối)
"IsActive":Boolean,//Trạngtháihoạtđộng
"Description":String,//Môtả
"Secret":String//MãbímậtđãđượcmãhoáBase64
}
}
3.Khicó sự kiệnphát sinh, KiotVietsẽ tạo rachữký từmãbí mật do bênđăng ký cung cấp
(secret)vàdữliệucủayêucầugửisangbênđăngký(requestbody),sauđótruyềnchữkýnàythông
quaheaderX-Hub-Signature, bênđăngký sẽcầnlấygiátrị chữký từheaderđểsokhớpvới chữ ký
do bên đăng ký tạo ra (bên đăng ký cũng sẽ tạo ra chữ ký dựa trên mã bí mật đã cung cấp cho
KiotVietvàdữliệudoKiotVietgửiđến).
4.Nếuchữkýkhôngkhớpthìbênđăngkýhãybỏquayêucầuđóvàkhôngxửlýnữa
5.Trảvềmãtrạngthái401nếuchữkýkhôngkhớp.
Đểantoànhơn,bênđăngkýnênlưutrữmãbímậtnàymộtcáchantoànvànênthườngxuyênthay
đổimãbímậtvàđồngbộmãnàyvớidịchvụwebhookcủaKiotViet.
Lưuý:Bênđăngkýhãylựachọnmãtrạngtháivàcáchxửlývớiyêucầukhônghợplệmộtcáchhợp
lý, vì trongtrường hợp yêu cầu gửi đếnlàtừphíaKiotViet vàviệc sokhớp chữ ký không như mong
muốn, dẫn đến bênđăng ký trả về một trongsố các mãtrạng thái 4xx (400, 401, 403, 404, 405) thì
dịchvụwebhookKiotVietsẽngưngviệcgửiyêucầutớiđiểmcuốiđó.
2.11.2. HuỷđăngkýWebhook
Mụcđíchsửdụng:HủyđăngkýWebhook
PhươngthứcvàURL:DELETEhttps://public.kiotapi.com/webhooks/{id}
Request:RequestsẽbaogồmIdcủawebhooktrongURL:
“id”:int//IDcủaWebhook
CôngTyCổphầnCôngnghệKiotViet 89/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Response:Trảlạithôngtinxóathànhcông(Code200)
{
"message":"Hủyđăngkýwebhookthànhcông"
}
2.11.3. Kháchhàng
customer.update
{
“Id”:string,
“Attempt”:int,
“Notifications”:[{
“Action”:string,
“Data”:[{
“Id”:long,
“Code”:string,
“Name”:string,
“Gender”:bool?,
“BirthDate”:Datetime?,
“ContactNumber”:string,
“Address”:string,
“LocationName”:string,
“Email”:string,
“ModifiedDate”:DateTime,
“Type”:byte?,
“Organization”:string,
“TaxCode”:string,
CôngTyCổphầnCôngnghệKiotViet 90/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“Comments”:string
}]
}]
}
customer.delete
{“RemoveId”:int[]}
2.11.4. Hànghóa
product.update
{
“Id”:string,
“Attempt”:int,
“Notifications”:[{
“Action”:string,
“Data”:[{
“Id”:long,
“Code”:string,
“Name”:string,
“FullName”:string,
“CategoryId”:int,
“CategoryName”:string,
“masterProductId”:long?,
“AllowsSale”:bool,
“HasVariants”:bool,
“BasePrice”:Decimal,
CôngTyCổphầnCôngnghệKiotViet 91/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“Weight”:double?,
“Unit”:string,
“MasterUnitId”:long?,
“ConversionValue”:double?,
“ModifiedDate”:DateTime?,
“Attributes”:[{
“ProductId”:long,
“AttributeName”:string,
“AttributeValue”:string
}],
“Units”:[{
“Id”:long,
“Code”:string,
“Name”:string,
“FullName”:string,
“Unit”:string,
“ConversionValue”:double,
“BasePrice”:Decimal
}],
“Inventories”:[{
“ProductId”:long,
“ProductCode”:string,
“ProductName”:string,
“BranchId”:int,
“BranchName”:string,
“Cost”:Decimal,
CôngTyCổphầnCôngnghệKiotViet 92/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“OnHand”:double,
“Reserved”:double
}],
“PriceBooks”:[{
“ProductId”:long,
“PriceBookId”:long,
“PriceBookName”:string,
“Price”:Decimal,
“IsActive”:bool,
“StartDate”:DateTime?,
“EndDate”:DateTime?
}],
“Images”:[{“Image”:string}]
}]
}]
}
product.delete
{“RemoveId”:int[]}
2.11.5. Tồnkho
stock.update
{
“Id”:string,
CôngTyCổphầnCôngnghệKiotViet 93/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“Attempt”:int,
“Notifications”:[{
“Action”:string,
“Data”:[{
“ProductId”:long,
“ProductCode”:string,
“ProductName”:string,
“BranchId”:int,
“BranchName”:string,
“Cost”:Decimal,
“OnHand”:double,
“Reserved”:double
}]
}]
}
2.11.6. Đặthàng
order.update
{
“Id”:string,
“Attempt”:int,
“Notifications”:[{
“Action”:string,
“Data”:[{
“Id”:long,
“Code”:string,
CôngTyCổphầnCôngnghệKiotViet 94/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“PurchaseDate”:DateTime,
“BranchId”:int,
“SoldById”:long?,
“SoldByName”:string,
“CustomerId”:long?,
“CustomerCode”:string,
“CustomerName”:string,
“Total”:Decimal,
“TotalPayment”:Decimal,
“Discount”:Decimal?,
“DiscountRatio”:double?
“Status”:int,
“StatusValue”:string,
“Description”:string,
“UsingCod”:bool
“ModifiedDate”:Datetime?
“OrderDetails”:[{
“ProductId”:long,
“ProductCode”:string,
“ProductName”:string,
“Quantity”:double,
“Price”:Decimal,
“Discount”:Decimal?,
“DiscountRatio”:double?
}]
}]
CôngTyCổphầnCôngnghệKiotViet 95/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}]
}
2.11.7. Hóađơn
invoice.update
{
“Id”:string,
“Attempt”:int,
“Notifications”:[{
“Action”:string,
“Data”:[{
“Id”:long,
“Code”:string,
“PurchaseDate”:DateTime,
“BranchId”:int,
“BranchName”:string,
“SoldById”:long,
“SoldByName”:string,
“CustomerId”:long?,
“CustomerCode”:string,
“CustomerName”:string,
“Total”:Decimal,
“TotalPayment”:Decimal,
“Discount”:Decimal?,
“DiscountRatio”:double?,
“Status”:byte,(1:hoànthành,2:đãhủy,3:đangxửlý:5:khônggiaođược)
CôngTyCổphầnCôngnghệKiotViet 96/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“StatusValue”:string,
“Description”:string,
“UsingCod”:bool,
“ModifiedDate”:DateTime?,
“InvoiceDelivery”:{
“DeliveryCode”:string,
“Status”:byte,(1:chưagiaohàng,2:đanggiaohàng,3:đãgiao
hàng,4:đangchuyểnhoàn,5đãchuyểnhoàn,6:đãhủy
“StatusValue”:string,
“Type”:byte?,
“Price”:Decimal?,
“Receiver”:string,
“ContactNumber”:string,
“Address”:string,
“LocationId”:int?,
“LocationName”:string,
“Weight”:double?,
“Length”:double?,
“Width”:double?,
“Height”:double?,
“PartnerDeliveryId”:long?,
“PartnerDelivery”:{
“Code”:string,
“Name”:string,
“ContactNumber”:string,
“Address”:string,
CôngTyCổphầnCôngnghệKiotViet 97/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“Email”:string
}
},
“InvoiceDetails”:[{
“ProductId”:long,
“ProductCode”:string,
“ProductName”:string,
“Quantity”:double,
“Price”:Decimal,
“Discount”:Decimal?,
“DiscountRatio”:double?
}],
“Payments”:[{
“Id”:long,
“Code”:string,
“Amount”:Decimal,
“AccountId”:int?,
“BankAccount”:string,
“Description”:string,
“Method”:string,
“Status”:byte?,
“StatusValue”:string,
“TransDate”:DateTime
}]
}]
}]
CôngTyCổphầnCôngnghệKiotViet 98/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
2.11.8. Bảnggiá
pricebook.update:Nhậnrequestkhicóthayđổithôngtincủabảnggiá
{
"Id":string,
"Attempt":int,
"Notifications":[{
"Action":string,
"Data":[{
"Id":long //Idbảnggiá
"Name":string //tênbảnggiá
"IsActive":bool, //trạngtháihoạtđộnghaykhông
"IsGlobal":bool, //cóphảilàbảnggiáchungkhông
"StartDate":date, //ngàybắtđầuápdụng
"EndDate":DateTime, //ngàyhếthạn
"ForAllCusGroup":bool, //ápdụngchotấtcảnhómkháchhàng
"ForAllUser":bool, //ápdụngchotấtcảuser
"PriceBookBranches":[{
"Id":long, //Idquanhệbảnggiá–chinhánh
"PriceBookId":long, //Idbảnggiá
"BranchId":long, //Idchinhánhápdụng,
"BranchName":string, //Tênchinhánhápdụng
}],
"PriceBookCustomerGroups":[{
"CustomerGroupName":string,
"Id":long, //Idquanhệbảnggiá–nhómkháchhàng
"PriceBookId":long, //Idbảnggiá
"CustomerGroupId":long, //Idnhómkháchhàng
}],
"PriceBookUsers":[{
CôngTyCổphầnCôngnghệKiotViet 99/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"UserName":string, //Tênngườidùng
"Id":long, //Idquanhệ
"PriceBookId":long, //Idbảnggiá
"UserId":long, //Idngườidùng
}],
}],
}]
}
pricebook.delete:Nhậnrequestkhicóbảnggiábịxóa
{
"Id":string,
"Attempt":int,
"Notifications":[{
"Action":string
"Data":[
long //Idbảnggiá
]
}]
}
pricebookdetail.update:Nhậnquestkhithôngtinhànghóatrongbảnggiáthayđổi(vídụ:thêm
hànghóavàobảnggiá)
{
"Id":string,
"Attempt":int,
"Notifications":[{
"Action":string
CôngTyCổphầnCôngnghệKiotViet 100/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"Data":[{
"PriceBookId":long, //Idbảnggiá
"ProductId":long, //Idhànghóa
"Price":decimal
}]
}]
}
pricebookdetail.delete:Nhậnrequestkhihànghóatrongbảnggiábịxóakhỏibảnggiá
{
"Id":string,
"Attempt":int,
"Notifications":[{
"Action":string,
"Data":[{
"PricebookId":long, //Idbảnggiá
"ProductIds":[
long //IDhànghóabịxóakhỏibảnggiá
]
}]
}]
}
2.11.9. Danhmụchànghóa
category.update
{
“Id”:string,
“Attempt”:int,
“Notifications”:[{
CôngTyCổphầnCôngnghệKiotViet 101/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“Action”:string,
“Data”:[{
"Id":int,
"Name":string,
"ParentId":int?,
"IsDeleted":bool,
"CreatedDate":Datetime,
"ModifiedDate":Datetime?,
"RetailerId":int,
"Rank":int,
"HasChild":bool
}]
}]
}]
}
category.delete
{
“RemoveId”:int[]
}
2.11.10. Chinhánh
branch.update
{
“Id”:string,
“Attempt”:int,
“Notifications”:[{
“Action”:string,
“Data”:[{
CôngTyCổphầnCôngnghệKiotViet 102/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"Id":long,
"Name":string,
"ContactNumber":string,
"SubContactNumber":string,
"Address":string,
"Location":string,
"WardName":string,
"IsActive":bool,
"IsLock":bool,
"CreatedDate":Datetime,
"ModifiedDate":Datetim?,
"RetailerId":int
}]
}]
}]
}
branch.delete
{
“RemoveId”:int[]
}
2.11.11. Danhsáchwebhook
Mụcđíchsửdụng:Trảvềthôngtindanhsáchwebhook
PhươngthứcvàURL:GEThttps://public.kiotapi.com/webhooks
Request:SửdụnghàmGET
Response:
{
“total”:int,
“pageSize”:int,//Sốitemstrong1trang,mặcđịnh20items,tốiđa100items
CôngTyCổphầnCôngnghệKiotViet 103/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“data”:[{
“id”:long,//Webhookid
“type”:string,//Loạiwebhook
“url”:string,//Địachỉđăngký
“isActive”:boolean,//Trạngtháihoạtđộng
"retailerId":int,//Idcửahàng
"description":string,//Môtả
"modifiedDate":datetime,//Thờigian
}]
}
2.11.12. Chitiếtwebhook
Mụcđíchsửdụng:TrảlạithôngtinchitiếtcủawebhooktheoId
PhươngthứcvàURL:GEThttps://public.kiotapi.com/webhooks/{id}
Request:SửdụnghàmGET
Response:
{
“id”:long,//Webhookid
“type”:string,//Loạiwebhook
“url”:string,//Địachỉđăngký
“isActive”:boolean,//Trạngtháihoạtđộng
"retailerId":int,//Idcửahàng
"description":string,//Môtả
}
2.12. Hóađơn
HiệntạiKiotViethỗtrợcácthiếtlậpchotínhnănghóađơnnhưsau:
 Trongtrườnghợpngườidùngkhôngtíchchọnsettingcho“Chophépbánhàngkhihếttồnkho”,
thì POST/PUT các API liên quan đến việc bán các sản phẩm đã hết tồn kho, trả lại thông báo
“Thiếtlập“Chophépbánhàngkhihếttồnkho”đangkhôngđượcbật”
CôngTyCổphầnCôngnghệKiotViet 104/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
 Trong trường hợp người dùng không tích chọn setting cho “Sử dụng tính năng giao hàng”, các
giao dịch liên quan tới giao hàng sẽ không hiển thị trên kiotviet nữa. Vì vậy khi gọi các API liên
quan tới phần giao hàng, cần trả lại thông báo “Thiết lập “Sử dụng tính năng giao hàng” đang
khôngđượcbật”.
 Trongtrườnghợpngườidùngtíchchọnsetting“Sửdụngínhnănggiaohàng”nhưngkhôngtích
chọnsettingcho “Quảnlýthuhộtiền”, các giaodịchliênquan tớithuhộtiềnsẽ khônghiểnthị
trên kiotviet nữa. Vì vậy khi gọi các API liên quan tới phần thu hộ tiền, cần trả lại thông báo
“Thiếtlập“Quảnlýthuhộtiền”đangkhôngđượcbật”.
 Trong trường hợp người dùng không tích chọn setting cho “Không cho phép thay đổi thời gian
bánhàng”,khiPost/PutcácAPIliênquanthờigianbánhàng,trảlạithôngbáo“Thiếtlập“Không
chophépthayđổithờigianbánhàng”đangkhôngđượcbật”.
MôtảchitiếtchocácAPIhỗtrợHóađơnnhưsau:
2.12.1. Lấydanhsáchhóađơn
Mụcđíchsửdụng:Trảvềdanhsáchhóađơntheocửahàngđãđượcxácnhận
PhươngthứcvàURL:GEThttps://public.kiotapi.com/invoices
Request:SửdụnghàmGETvớithamsố:
“branchIds”:int[],optional//IDchinhánh
“customerIds”:long[],optional//Idkháchhàng
“customerCode”:string//Mãkháchhàng
“status”:int[],optional//Tìnhtrạnghóađơn
“includePayment”:Boolean,//cólấythôngtinthanhtoán
“includeInvoiceDelivery”:Boolean,//hóađơncógiaohànghaykhông
“lastModifiedFrom”:datetime?//thờigiancậpnhật
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int,
“lastModifiedFrom”:datetime?//thờigiancậpnhật
“toDate”:datetime?//ThờigiancậpnhậtchođếnthờiđiểmtoDate
“orderBy”:string,//SắpxếpdữliệutheotrườngorderBy(Vídụ:orderBy=name)
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
CôngTyCổphầnCôngnghệKiotViet 105/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“orderDirection”: string, //Sắp xếp kết quả trả về theo: Tăng dần Asc (Mặc định), giảm
dầnDesc
“orderId”:long?,//LọcdanhsáchhóađơntheoIdcủađơnhóađơn
“createdDate”:datetime?//Thờigiantạo
“fromPurchaseDate”:datetime?//Từngàygiaodịch
“toPurchaseDate”:datetime?//Đếnngàygiaodịch
Response:
{
“total”:int,
“pageSize”:int,
“data”:[{
“id”:long//Idhóađơn
“code”:string//Mãhóađơn
“purchaseDate”:datetime//Ngàyhóađơn
“branchId”:int,//Idchinhánh
“branchName”:string,//Tênchinhánh
“soldById”:long?,
“soldByName”:string
“customerId”:long?,//Idkháchhàng
“customerCode”:string,Mãkháchhàng
“customerName”:string,//Tênkháchhàng
“total”:decimal,//Kháchcầntrả
“totalPayment”:decimal,//Kháchđãtrả
“status”:int,//trạngtháihóađơn
“statusValue”:string,//trạngtháihóađơnbằngchữ
CôngTyCổphầnCôngnghệKiotViet 106/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“usingCod”:boolean,
“createdDate”:datetime,//Ngàytạo
“modifiedDate”:datetime,//Ngàycậpnhật
“payments”:[{
“id”:long,
“code”:string,
“amount”:decimal,
“method”:string”,
“status”:byte?,
“statusValue”:string,
“transDate”:datetime,
“bankAccount”:string,
“accountId”:int?
}],
"invoiceOrderSurcharges":[ {
"id":long,
"invoiceId":long?,
"surchargeId":long?,
"surchargeName":string,
"surValue":decimal?,
"price":decimal?,
"createdDate":DateTime
}
],
“invoiceDetails”:{
“productId”:long,//Idhànghóa
CôngTyCổphầnCôngnghệKiotViet 107/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“productCode”:string,
“productName”:string,//Tênhànghóa
(baogồmthuộctínhvàđơnvịtính)
“quantity”:double,//Sốlượnghànghóa
“price”:decimal,//Giátrị
“discountRatio”:double?,//Giảmgiátrênsảnphẩmtheo%
“discount”:decimal?,//Giảmgiátrênsảnphẩmtheotiền
“note”:string//Ghichúhànghóa
"serialNumbers":string,//Danhsáchimei
"productBatchExpire":{
"id":long,//Idlô
"productId"long,//IDsảnphẩm
"batchName":string,//Tên
"fullNameVirgule":string,//Tênđầyđủ
"createdDate":DateTime,//Ngàytạolô
"expireDate":DateTime//Ngàyhếthạnlô
}
},
“SaleChannel”:{
“IsNotDelete”:bool,
“RetailerId”:long,
“Position”:int,
“IsActivate”:bool,
“CreatedBy”:long,
“CreatedDate”:datetime,
“Id”:long,
CôngTyCổphầnCôngnghệKiotViet 108/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“Name”:string
},//ĐểlấythôngtinSaleChannelthìphảitruyềnthêm
//SaleChannel=true
“invoiceDelivery”:{
“deliveryCode”:string,
“type”:byte?,
“status”:byte,(1:Chờxửlý,2:Đanggiaohàng,3:Giaothànhcông,4:Đangchuyểnhoàn,
5:Đãchuyểnhoàn,6:Đãhủy,7:Đanglấyhàng,8:Chờlấylại,9:Đãlấyhàng,10:Chờgiaolại,11:Chờ
chuyểnhàng,12:Chờchuyểnhoànlại)//trạngtháivậnđơn
“statusValue”:string,
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
“address”:string,
“locationId”:int?,
“locationName”:string,
“usingPriceCod”:bool,//Thuhộtiền
“priceCodPayment”:decimal,//Sốtiềnthuhộ
“weight”:double?,
“length”:double?,
“width”:double?,
“height”:double?,
“partnerDeliveryId”:long?,
“partnerDelivery”:{
“code”:string,
“name”:string,
CôngTyCổphầnCôngnghệKiotViet 109/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“address”:string,
“contactNumber”:string,
“email”:string
}
}
}]
}
2.12.2. Lấychitiếthóađơn
Mụcđíchsửdụng:TrảvềthôngtinchitiếtcủahóađơntheoID,theoCode
PhươngthứcvàURL:
- TheoId:GEThttps://public.kiotapi.com/invoices/{id}
- TheoCode:GEThttps://public.kiotapi.com/invoices/code/{code}
Request:SửdụnghàmGETvớithamsố:
“id”:long//IDcủahóađơn
“code”:string//Mãcủahóađơn
Response:
{
“id”:long//Idhóađơn
“code”:string//Mãhóađơn
“orderCode”:string//Mãhóađơn
“purchaseDate”:datetime//Ngàyhóađơn
“branchId”:int,//Idchinhánh
“branchName”:string,//Tênchinhánh
“soldById”:long?,
“soldByName”:string
CôngTyCổphầnCôngnghệKiotViet 110/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“customerId”:long?,//Idkháchhàng
“customerCode”:string,//Mãkháchhàng
“customerName”:string,//Tênkháchhàng
“total”:decimal,//Kháchcầntrả
“totalPayment”:decimal,//Kháchđãtrả
“status”:int,//trạngtháiđơnhóađơn
“statusValue”:string,//trạngtháiđơnhóađơnbằngchữ
“description”:string,//ghichú
“usingCod”:boolean,
“createdDate”:datetime,//Ngàytạo
“modifiedDate”:datetime,//Ngàycậpnhật
“payments”:[{
“id”:long,
“code”:string,
“amount”:decimal,
“method”:string,
“status”:byte?,
“statusValue”:string,
“transDate”:datetime,
“bankAccount”:string,
“accountId”:int?
}],
"invoiceOrderSurcharges":[ {
"id":long,
"invoiceId":long?,
"surchargeId":long?,
CôngTyCổphầnCôngnghệKiotViet 111/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"surchargeName":string,
"surValue":decimal?,
"price":decimal?,
"createdDate":DateTime
}
],
“invoiceDetails”:{
“productId”:long,//Idhànghóa
“productCode”:string,
“productName”:string,//Tênhànghóa
(baogồmthuộctínhvàđơnvịtính)
“quantity”:double,//Sốlượnghànghóa
“price”:decimal,//Giátrị
“discountRatio”:double?,//Giảmgiátrênsảnphẩmtheo%
“discount”:decimal?,//Giảmgiátrênsảnphẩmtheotiền
“note”:string//Ghichúhànghóa
"serialNumbers":string,//Danhsáchimei
"productBatchExpire":{
"id":long,//Idlô
"productId"long,//IDsảnphẩm
"batchName":string,//Tên
"fullNameVirgule":string,//Tênđầyđủ
"createdDate":DateTime,//Ngàytạolô
"expireDate":DateTime//Ngàyhếthạnlô
}
},
CôngTyCổphầnCôngnghệKiotViet 112/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“invoiceDelivery”:{
“deliveryCode”:string,
“type”:byte?,
“status”:byte,(1:chưagiaohàng,2:đanggiaohàng,3:đãgiaohàng,4:đangchuyểnhoàn,
5đãchuyểnhoàn,6:đãhủy)//trạngtháivậnđơn
“statusValue”:string,
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
“address”:string,
“locationId”:int?,
“locationName”:string,
“usingPriceCod”:bool,//Thuhộtiền
“priceCodPayment”:decimal,//Sốtiềnthuhộ
“weight”:double?,
“length”:double?,
“width”:double?,
“height”:double?,
“partnerDeliveryId”:long?,
“partnerDelivery”:{
“code”:string,
“name”:string,
“address”:string,
“contactNumber”:string,
“email”:string
},
CôngTyCổphầnCôngnghệKiotViet 113/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“SaleChannel”:{
“IsNotDelete”:bool,
“RetailerId”:long,
“Position”:int,
“IsActivate”:bool,
“CreatedBy”:long,
“CreatedDate”:datetime,
“Id”:long,
“Name”:string
}
}
}
2.12.3. Thêmmớihóađơn
Mụcđíchsửdụng:Tạomớihóađơn
PhươngthứcvàURL:POSThttps://public.kiotapi.com/invoices
Request:JSONmãhóayêucầugồm1objecthóađơn:
{
“branchId”:int,
“isApplyVoucher”:true,//Cóapplyvoucherkhitạohóađơnkhông,
“purchaseDate”:datetime,
“customerId”:long?,
“discount”:decimal?,
“totalPayment”:decimal,
“saleChannelId”:int?optional//Idkênhbánhàng,nếukhôngtruyềnmặcđịnhkênhkhác
“method”:string,
CôngTyCổphầnCôngnghệKiotViet 114/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“accountId”:int?,
“usingCod”:bool,
“soldById”:long,
“orderId”:long?,
“invoiceDetails”:[{
“productId”:long,
“productCode”:string,
“productName”:string,
“quantity”:double,
“price”:decimal,
“discount”:decimal?,
“discountRatio”:decimal?,
“note”:string
“serialNumbers”:string,//DanhsáchserialImeidạngchuỗi,mỗiImeicáchnhauởidấu
phẩy(,),vídụ:"ABC,XYZ"
}],
“deliveryDetail”:{
“deliveryCode”:string,
“type”:byte?,
“status”:byte,(1:Chờxửlý,2:Đanggiaohàng)//trạngtháivậnđơn
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
“address”:string,
“locationId”:int,
“locationName”:string
CôngTyCổphầnCôngnghệKiotViet 115/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“wardName”:string,//Tênphường
“weight”:double,
“length”:double,
“width”:double,
“usingPriceCod”:bool,//Thuhộtiền
“height”:double,
“partnerDeliveryId”:long?,
“expectedDelivery”:datetime,
“partnerDelivery”:{
“code”:string,
“name”:string,
“address”:string,
“contactNumber”:string,
“email”:string
},
“surchages”:[{
“id”:int,
“code”:string,
“price”:decimal,
}]},
“customer":{
“id”:long,
"code":string,
"name":string,
"gender":boolean,
"birthDate":datetime,
"contactNumber":string,
"address":string,
CôngTyCổphầnCôngnghệKiotViet 116/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"email":string,
"comment":string
},
}
}
Response:
{
“id”:long,
“code”:string,
“purchaseDate”:datetime,
“branchId”:int,
“branchName”:string,
“soldById”:long?,
“soldByName”:string,
“customerId”:long?,
“customerName”:string,
“saleChannelId”:int?,optional//Idkênhbánhàng,nếukhôngtruyềnmặcđịnhkênhkhác
“total”:decimal,//Kháchcầntrả
“totalPayment”:decimal,//Kháchđãtrả
“discountRatio”:double?,//Giảmgiátrênđơntheo%
“discount”:decimal?,//Giảmgiátrênđơntheotiền
“method”:string,//Phươngthứcthanhtoán(Cash,Card,Transfer)
“status”:int,//trạngtháihóađơn
“statusValue”:string,//trạngtháihóađơnbằngchữ
“description”:string,//ghichú
CôngTyCổphầnCôngnghệKiotViet 117/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"usingCod":boolean,
“invoiceDetails”:{
“productId”:long,//Idhànghóa
“productName”:string,//Tênhànghóa
(baogồmthuộctínhvàđơnvịtính)
“quantity”:double,//Sốlượnghànghóa
“price”:decimal,//Giátrị
“discountRatio”:double?,//Giảmgiátrênsảnphẩmtheo%
“discount”:decimal?,//Giảmgiátrênsảnphẩmtheotiền,
“note”:string//Ghichúhànghóa
},
“deliveryDetail”:{
“deliveryCode”:string,
“type”:byte?,
“status”:byte,(1:chưagiaohàng,2:đanggiaohàng)//trạngtháivậnđơn
“statusValue”:string,
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
“address”:string,
“locationId”:int?,
“locationName”:string,
“usingPriceCod”:bool,//Thuhộtiền
“priceCodPayment”:decimal,//Sốtiềnthuhộ “weight”:double?,
CôngTyCổphầnCôngnghệKiotViet 118/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“length”:double?,
“width”:double?,
“height”:double?,
“partnerDeliveryId”:long?,
“partnerDelivery”:{
“code”:string,
“name”:string,
“address”:string,
“contactNumber”:string,
“email”:string
}
},
"invoiceOrderSurcharges":[ {
"id":long,
"invoiceId":long?,
"surchargeId":long?,
"surchargeName":string,
"surValue":decimal?,
"price":decimal?,
"createdDate":DateTime
}
],
}
2.12.4. Cậpnhậthóađơn
Mụcđíchsửdụng:CậpnhậthóađơntheoID
CôngTyCổphầnCôngnghệKiotViet 119/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
PhươngthứcvàURL:PUThttps://public.kiotapi.com/invoices/Id
Request:SửdụnghàmPUTvớiIDhóađơnqua1objectJSON.
“id”:long//IDhóađơn
Body
{
“purchaseDate”:datetime
“status”:byte,
“soldById”:long,
“codPaymentMethod”:string,//Phươngthứcthanhtoánthuhộ(Cash,Tranfer),
“codPaymentAccount”:int?,//Idtàikhoảnngânhàngnếuthanhtoánchuyểnkhoản,thẻngân
hang,
“saleChannelId”:int?optional//Idkênhbánhàng,nếukhôngtruyềnmặcđịnhkênhkhác
“deliveryDetail”:{
“deliveryCode”:string,
“type”:byte?,
“status”:byte,(1:Chờxửlý,2:Đanggiaohàng,3:Giaothànhcông,4:Đangchuyểnhoàn,
5:Đãchuyểnhoàn,6:Đãhủy,7:Đanglấyhàng,8:Chờlấylại,9:Đãlấyhàng,10:Chờgiaolại,11:Chờ
chuyểnhàng,12:Chờchuyểnhoànlại)//trạngtháivậnđơn
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
“address”:string,
“locationId”:int,
“locationName”:string,
“wardName”:string,//Tênphường
“weight”:double,
“length”:double,
CôngTyCổphầnCôngnghệKiotViet 120/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“usingPriceCod”:bool,//Thuhộtiền
“priceCodPayment”:decimal,//Sốtiềnthuhộ
“width”:double,
“height”:double,
“partnerDeliveryId”:long?,
“expectedDelivery”:datetime,
“partnerDelivery”:{
“code”:string,
“name”:string,
“address”:string,
“contactNumber”:string,
“email”:string
}
}
}
Response:
{
“id”:long,
“code”:string,
“purchaseDate”:datetime,
“branchId”:int,
“branchName”:string
“soldById”:long?,
“soldByName”:string,
“customerId”:long?,
CôngTyCổphầnCôngnghệKiotViet 121/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“customerName”:string,
“total”:decimal,//Kháchcầntrả
“totalPayment”:decimal,//Kháchđãtrả
“discountRatio”:double?,//Giảmgiátrênđơntheo%
“discount”:decimal?,//Giảmgiátrênđơntheotiền
“method”:string,//Phươngthứcthanhtoán(Cash,Card,Transfer)
“status”:int,//trạngtháiđơnhóađơn
“statusValue”:string,//trạngtháihóađơnbằngchữ
“description”:string,//ghichú
"usingCod":boolean,
“saleChannelId”:int?,
“invoiceDetails”:{
“productId”:long,//Idhànghóa
“productName”:string,//Tênhànghóa
(baogồmthuộctínhvàđơnvịtính)
“quantity”:double,//Sốlượnghànghóa
“price”:decimal,//Giátrị
“discountRatio”:double?,//Giảmgiátrênsảnphẩmtheo%
“discount”:decimal?,//Giảmgiátrênsảnphẩmtheotiền
},
“deliveryDetail”:{
“deliveryCode”:string,
“type”:byte?,
“status”:byte,(1:Chờxửlý,2:Đanggiaohàng,3:Giaothànhcông,4:Đangchuyểnhoàn,
5:Đãchuyểnhoàn,6:Đãhủy,7:Đanglấyhàng,8:Chờlấylại,9:Đãlấyhàng,10:Chờgiaolại,11:Chờ
chuyểnhàng,12:Chờchuyểnhoànlại)//trạngtháivậnđơn
CôngTyCổphầnCôngnghệKiotViet 122/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“statusValue”:string,
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
“address”:string,
“locationId”:int?,
“locationName”:string,
“wardName”:string,//Tênphường
“usingPriceCod”:bool,//Thuhộtiền
“priceCodPayment”:decimal,//Sốtiềnthuhộ
“weight”:double?,
“length”:double?,
“width”:double?,
“height”:double?,
“partnerDeliveryId”:long?,
“partnerDelivery”:{
“code”:string,
“name”:string,
“address”:string,
“contactNumber”:string,
“email”:string
}
}
}
CôngTyCổphầnCôngnghệKiotViet 123/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
2.12.5. Xóahóađơn
Mụcđíchsửdụng:XóahóađơntheoID
PhươngthứcvàURL:DELETEhttps://public.kiotapi.com/invoices
Request::JSONmãhóayêucầugồm1objectthamsốsau:
{
“id”:long//IDcủahóađơn
“isVoidPayment”:bool//Hủyphiếuthanhtoángắnkèmhóađơn,nếukhôngtruyềntham
sốnàythìmặcđịnhkhônghủyphiếuthanhtoángắnkèmhóađơn
}
Response:
{
“id”:long,
“code”:string,
“purchaseDate”:datetime,
“branchId”:int,
“branchName”:string,
“soldById”:long?,
“soldByName”:string,
“customerId”:long?,
“customerName”:string,
“saleChannelId”:int?,optional//Idkênhbánhàng
“total”:decimal,//Kháchcầntrả
“totalPayment”:decimal,//Kháchđãtrả
“discountRatio”:double?,//Giảmgiátrênđơntheo%
CôngTyCổphầnCôngnghệKiotViet 124/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“discount”:decimal?,//Giảmgiátrênđơntheotiền
“method”:string,//Phươngthứcthanhtoán(Cash,Card,Transfer)
“status”:2,//trạngtháiđơn
“statusValue”:“Đãhủy”,//trạngtháiđơnhóađơnbằngchữ
“description”:string,//ghichú
"usingCod":boolean,
“invoiceDetails”:{
“productId”:long,//Idhànghóa
“productName”:string,//Tênhànghóa
(baogồmthuộctínhvàđơnvịtính)
“quantity”:double,//Sốlượnghànghóa
“price”:decimal,//Giátrị
“discountRatio”:double?,//GiảmgiátrênSPtheo%
“discount”:decimal?,//GiảmgiátrênSPtheotiền,
“note”:string//Ghichúhànghóa
},
“deliveryDetail”:{
“deliveryCode”:string,
“type”:byte?,
“status”:byte,(1:Chờxửlý,2:Đanggiaohàng,3:Giaothànhcông,4:Đangchuyểnhoàn,
5:Đãchuyểnhoàn,6:Đãhủy,7:Đanglấyhàng,8:Chờlấylại,9:Đãlấyhàng,10:Chờgiaolại,11:Chờ
chuyểnhàng,12:Chờchuyểnhoànlại)//trạngtháivậnđơn
“statusValue”:string,
“price”:Decimal?,
“receiver”:string,
“contactNumber”:string,
CôngTyCổphầnCôngnghệKiotViet 125/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“address”:string,
“locationId”:int?,
“locationName”:string,
“usingPriceCod”:bool,//Thuhộtiền
“priceCodPayment”:decimal,//Sốtiềnthuhộ
“weight”:double?,
“length”:double?,
“width”:double?,
“height”:double?,
“partnerDeliveryId”:long?,
“partnerDelivery”:{
“code”:string,
“name”:string,
“address”:string,
“contactNumber”:string,
“email”:string
}
},
“payments”:[
{
“id”:string,
“code”:string,
“amount”:decimal,
“method”:string,
“status”:byte,
“statusValue”:byte,
CôngTyCổphầnCôngnghệKiotViet 126/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“transDate”:datetime
}],
"invoiceOrderSurcharges":[]
}
2.13. Nhómkháchhàng
2.13.1. Lấydanhsáchnhómkháchhàng
Mụcđíchsửdụng: lấydanhsáchnhómkháchhàng
PhươngthứcvàURL:GEThttps://public.kiotapi.com/customers/group
Response:
{
"total":int//Tổngdanhsáchnhóm
"data":[
{
"id":int//Idnhómkháchhàng
"name":string//Tênnhómkháchhàng,
"description":string//Ghichú,
"createdDate":DateTime//Ngàytạo,
"createdBy":long//Idngườitạo,
"retailerId":int//Idchinhánh,
"discount":decimal?//Giảmgiá,
"customerGroupDetails":[
{
"id":long//IdChitiếtnhómkháchhàng
"customerId":long//Idkháchhàng
"groupId":int//Idnhómkháchhàng
CôngTyCổphầnCôngnghệKiotViet 127/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
]
}]
}
2.14. Sổquỹ
2.14.1. Lấydanhsáchsổquỹ
Mụcđíchsửdụng: Trảvề danh sáchphiếu thuchitrongsổquỹ theocửahàng đãđược xác
nhận
PhươngthứcvàURL:GEThttps://public.kiotapi.com/cashflow
Request:SửdụnghàmGETvớithamsố:
“branchIds”:int[],optional//IDchinhánh
“code”:string[]//Danhsáchmãcodecủaphiếu
“userId”:long?//Idngườitạo
“accountId”:int?//Tàikhoảnnhận
“partnerType”: srting //Loại người nộp/nhận: A: tất cả, C: khách hàng, S: nhà cung
cấp,U:nhânviên,D:tốitácgiaohàng,O:khác
“method”:string[]//Danhsáchphươngthứcthanhtoán
“cashFlowGroupId”:int?[]//Loạithu/chi
“usedForFinancialReporting”: int? //Lọc theo kết qua kinh doanh: 0: không hoạch
toán,1:đưavàohoạchtoán
“partnerName”:string//Tênngườinộp/nhận
“contactNumber”:string//Sốđiệnthoạingườinộp/nhận
“isReceipt”:bool?//Theophiếuthu/chi;True:thu,false:chi
“includeAccount”:bool//Lấythôngtintàikhoảnngânhànghaykhông
CôngTyCổphầnCôngnghệKiotViet 128/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“includeBranch”:bool?//Lấytênchinhánhhaykhông
“includeUser”:bool?//Lấytênngườitạohaykhông
“startDate”:datetime?//thờigianbắtđầu
“endDate”:datetime?//thờigiankếtthúc
“status”:int?//trạngtháiphiếu;0:Đãthanhtoán,1:Đãhủy,khôngtruyền:tấtcả
“ids”:long?[]//Idphiếuthu/chi
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
Response:
{
“total”:int,
“pageSize”:int,
“data”:[{
“id”:long//Idphiếu
“code”:string//Mãphiếu
“address”:string//Địachỉ
“branchId”:int,//Idchinhánh
“wardName”:string,//Tênphường
“contactNumber”:string,//Sốđiệnthoại
“createdBy”:long//Idngườitạo
“usedForFinancialReporting”:int,
“cashFlowGroupId”:int?,Idloạithuchi
“method”:string,//phươngthứcthanhtoán
“partnerType”:string,//Ngườinộp/nhận
“partnerId”:long?,//Idngườinộp/nhận
“status”:int,//trạngtháiphiếu
“statusValue”:string,//trạngtháiphiếubằngchữ
CôngTyCổphầnCôngnghệKiotViet 129/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“transDate”:datetime,//Ngàytạo
“amount”:decimal,//Giátrị
“partnerName”:string,//tênngườinộp/nhận
“user”:string,//tênngườitạo
“AccountId”:int?//Idtàikhoảnngânhàng
“Description”:string//Ghichú
}]
}
2.14.2. Thanhtoánhóađơn
Mụcđíchsửdụng:Thutiềnhóađơnđơnnợ
Phươngthức:POST
URL:https://public.kiotapi.com/payments
Request:
{
"amount":decimal,//Sốtiền
"method":string,//Phươngthứcthanhtoán(Cash,Card,Transfer)
"accountId":int?,optional//IDtàikhoảnngânhàng,truyềnnếuphươngthứcthanhtoánlàCard,
Transfer
"invoiceId":long//IDhóađơn
}
Response:
{
"paymentId":int,//IDphiếuthanhtoán
"paymentCode":string,//Mãphiếuthanhtoán
CôngTyCổphầnCôngnghệKiotViet 130/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"amount":decimal,//Sốtiền
"method":string,//Phươngthứcthanhtoán(Cash,Card,Transfer)
"accountId":int?,//IDsốtàikhoảnngânhàng,truyềnnếuphươngthứcthanhtoánlàCard,
Transfer
"invoiceId":long,//IDhóađơn
"DocumentCode":long//Mãhóađơn
}
2.15. Nhậphàng
2.15.1. Lấydanhsáchnhậphàng
Mụcđíchsửdụng:Trảvềdanhsáchnhậphàng
PhươngthứcvàURL:GEThttps://public.kiotapi.com/purchaseorders
Request:SửdụnghàmGETvớithamsố:
“branchIds”:int[],optional//IDchinhánh
“status”:int[],optional//Tìnhtrạngđặthàng
“includePayment”:Boolean,//cólấythôngtinthanhtoán
“includeOrderDelivery”:Boolean,
"fromPurchaseDate":"date",optinal//từngàynhậphàng
"toPurchaseDate":"date",optinal//đếnngàynhậphàng
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
Response:
“total”:int,
“pageSize”:int,
“data”:[{
“id”:long//Idphiếu
CôngTyCổphầnCôngnghệKiotViet 131/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“code”:string//Mãphiếu
“branchId”:int,//Idchinhánh
“branchName”:string,//Tênchinhánh
“purchaseDate”:datetime,//Ngàynhậphàng
“discountRatio”:long//Giảmgiáphầntrăm
“total”:int,//Giátrịnhậphàng
“supplierId”:long,//Idnhàcungcấp
“supplierName”:string,//Tênnhàcungcấp
“supplierCode”:string,//Mãnhàcungcấp
“partnerType”:string,//Ngườinộp/nhận
“purchaseById”:long?,//Idngườinhập
“purchaseName”:int,//tênngườinhập
“purchaseOrderDetails”:[{
“productId”:long,//Idhànghóa
“ProductCode”:string,//mãhànghóa
“productName”:string,//Tênhànghóa
“quantity”:double,//Sốlượnghànghóa
“price”:decimal,//Giátrị
“discount”:string,//Giảmgiá
"serialNumbers":string,//Danhsáchimei
"productBatchExpire":{
"id":long,//Idlô
"productId"long,//IDsảnphẩm
"batchName":string,//Tên
"fullNameVirgule":string,//Tênđầyđủ
"createdDate":DateTime,//Ngàytạolô
CôngTyCổphầnCôngnghệKiotViet 132/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"expireDate":DateTime//Ngàyhếthạnlô
}
}]//Thôngtinnhậphàngchitiết
}]
}
2.15.2. Lấychitiếtnhậphàng
Mụcđíchsửdụng:TrảlạichitiếtcủamộtsảnphẩmcụthểtheoID
PhươngthứcvàURL:GEThttps://public.kiotapi.com/purchaseorders/{id}
Request:SửdụnghàmGETvớithamsố:
“id”:long//IDcủacủanhậphàng
Response:
{
“id”:long//Idphiếu
“retailerId”:long//Idshope
“code”:string//Mãphiếu
“branchId”:int,//Idchinhánh
“branchName”:string,//Tênchinhánh
“purchaseDate”:datetime,//Ngàynhậphàng
“discountRatio”:long//Giảmgiáphầntrăm
“total”:int,//Giátrịnhậphàng
“supplierId”:long,//Idnhàcungcấp
“supplierName”:string,//Tênnhàcungcấp
“supplierCode”:string,//Mãnhàcungcấp
“partnerType”:string,//Ngườinộp/nhận
CôngTyCổphầnCôngnghệKiotViet 133/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“purchaseById”:long?,//Idngườinhập
“purchaseName”:int,//tênngườinhập
“purchaseOrderDetails”:[{
“productId”:long,//Idhànghóa
“ProductCode”:string,//mãhànghóa
“productName”:string,//Tênhànghóa
“quantity”:double,//Sốlượnghànghóa
“price”:decimal,//Giátrị
“discount”:string,//Giảmgiá
"serialNumbers":string,//Danhsáchimei
"productBatchExpire":{
"id":long,//Idlô
"productId"long,//IDsảnphẩm
"batchName":string,//Tên
"fullNameVirgule":string,//Tênđầyđủ
"createdDate":DateTime,//Ngàytạolô
"expireDate":DateTime//Ngàyhếthạnlô
}
}]//Thôngtinnhậphàngchitiết
“payments”:[{
“id”:long,//Idthanhtoán
“code”:string,//mãthanhtoán
“method”:string,//phươngthứcthanhtoán
“status”:int,//trạngthái
“statusValue”:string,//têntrạngthái
“transDate”:DateTime,//ngàyThanhtoán
CôngTyCổphầnCôngnghệKiotViet 134/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}]//Thôngtinthanhtoán
}
2.15.3. Thêmmớinhậphàng
Mụcđíchsửdụng:Tạomớiphiếunhậphàng
PhươngthứcvàURL:POSThttps://public.kiotapi.com/purchaseorders
Request:JSONmãhóayêucầugồm1objectnhậphàng
{
“purchaseDate”:datetime, //Ngàynhậphàng
“branchId”:int, //Idchinhánh
“supplier”:{
“code”:string,
“name”:string,
“contactNumber”:string,
“address”:string,
“email”:string,
“comment”:string
},//ThôngtinNhàcungcấp
“description”:string //Ghichúphiếunhập
“isDraft”:int //Trạngtháicủaphiếunhập
“discount”:decimal? //Sốtiềngiảmgiá
“discountRatio”:double? //Phầntrămgiảmgiá
“paidAmount”:decimal, //TiềntrảtrướcchoNCC
“paymentMethod”:string, //PhươngthứcthanhtoánchoNCC(Cash,Transfer,Card)
CôngTyCổphầnCôngnghệKiotViet 135/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"accountId":long //IdcủatàikhoảnngânhàngnếuphươngthứcthanhtoánlàTRANSFER,
CARD(lấythôngtintừhttps://public.kiotapi.com/BankAccounts)
"surcharges”:[{
“code”:string, //Mãchiphí
“name”:string, //Tênchiphí
“value”:decimal?, //Sốtiềnthu
“valueRatio”:decimal?, //Phầntrămthu
“isSupplierExpense”:bool,//Hoànlạikhitrảhàngnhập
“type”:int, //Hìnhthức,chiphínhậptrảnhàcungcấp?
}]//Danhsáchthukhác,baogồmchiphítrảnhàcungcấphoặckhôngtrảnhàcungcấp.
“purchaseOrderDetails”:[{
“productCode”:string, //Mãhànghóa
“description”:string, //Ghichú
“quantity”:double, //Sốlượnghànghóa
“price”:decimal, //Giánhập
“discount”:decimal?, //Giảmgiá
“discountRatio”:double?,//Giảmgiá
}]//Thôngtinnhậphàngchitiết
}
Response:
{
“id”:long //Idphiếu
“retailerId”:long //Idshop
CôngTyCổphầnCôngnghệKiotViet 136/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“code”:string //Mãphiếu
“branchId”:int, //Idchinhánh
“branchName”:string, //Tênchinhánh
“purchaseDate”:datetime, //Ngàynhậphàng
“discount”:decimal //Sốtiềngiảmgiá
“discountRatio”:long //Phầntrămgiảmgiá
“total”:int, //Giátrịnhậphàng
“supplierId”:long, //Idnhàcungcấp
“supplierName”:string, //Tênnhàcungcấp
“supplierCode”:string, //Mãnhàcungcấp
“partnerType”:string, //Ngườinộp/nhận
“purchaseById”:long?, //Idngườinhập
“purchaseName”:int, //Tênngườinhập
“purchaseOrderDetails”:[{
“productId”:long, //Idhànghóa
“productCode”:string, //Mãhànghóa
“productName”:string, //Tênhànghóa
“quantity”:double, //Sốlượnghànghóa
“price”:decimal, //Giánhập
“discount”:decimal, //Giảmgiá
“discountRatio”:double?,//Giảmgiá
}]//Thôngtinnhậphàngchitiết
“payments”:[{
“id”:long, //Idthanhtoán
“code”:string, //Mãthanhtoán
“method”:string, //Phươngthứcthanhtoán,
CôngTyCổphầnCôngnghệKiotViet 137/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“amount”:decimal, //Phươngthứcthanhtoán,
“status”:int, //Trạngthái
“statusValue”:string,//Têntrạngthái
“transDate”:DateTime,//NgàyThanhtoán
}]//Thôngtinthanhtoán
}
2.15.4. Cậpnhậtnhậphàng
Mụcđíchsửdụng:CậpnhậtphiếunhậphàngtheoID
PhươngthứcvàURL:PUThttps://public.kiotapi.com/purchaseorders/Id
Request:SửdụnghàmPUTvớiIDphiếunhậphàngqua1objectJSON.
“id”:long//IDphiếunhậphàng
Body
{
“purchaseDate”:datetime, //Ngàynhậphàng
“branchId”:int, //Idchinhánh
“supplier”:{
“code”:string,
“name”:string,
“contactNumber”:string,
“address”:string,
“email”:string,
“comment”:string
},//ThôngtinNhàcungcấp
“description”:string //Ghichúphiếunhập
CôngTyCổphầnCôngnghệKiotViet 138/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“isDraft”:bool //Trạngtháicủaphiếunhập.
“discount”:decimal? //Sốtiềngiảmgiá
“discountRatio”:double? //Phầntrămgiảmgiá
“paidAmount”:decimal, //TiềntrảtrướcchoNCC
“paymentMethod”:string, //PhươngthứcthanhtoánchoNCC(Cash,Transfer,Card)
"accountId”:long, //Idaccounttàikhoảnngânhàngnếuphươngthứcthanhtoánlà
TRANSFER,CARD,
"surcharges”:[{
“code”:string, //Mãchiphí
“name”:string, //Tênchiphí
“value”:decimal?, //Sốtiềnthu
“valueRatio”:decimal?, //Phầntrămthu
“isSupplierExpense”:bool,//Hoànlạikhitrảhàngnhập
“type”:int, //Hìnhthức,chiphínhậptrảnhàcungcấp?
}]//Danhsáchthukhác,baogồmchiphítrảnhàcungcấphoặckhôngtrảnhàcungcấp.
“purchaseOrderDetails”:[{
“productCode”:string, //Mãhànghóa
“description”:string, //Ghichú
“quantity”:double, //Sốlượnghànghóa
“price”:decimal, //Giánhập
“discount”:decimal?, //Sốtiềngiảmgiá
“discountRatio”:double?,//Phầntrămgiảmgiá
}]//Thôngtinnhậphàngchitiết
CôngTyCổphầnCôngnghệKiotViet 139/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
Response:
{
“id”:long //Idphiếu
“retailerId”:long //Idshop
“code”:string //Mãphiếu
“branchId”:int, //Idchinhánh
“branchName”:string, //Tênchinhánh
“purchaseDate”:datetime, //Ngàynhậphàng
“discount”:decimal //Sốtiềngiảmgiá
“discountRatio”:long //Phầntrămgiảmgiá
“total”:int, //Giátrịnhậphàng
“supplierId”:long, //Idnhàcungcấp
“supplierName”:string, //Tênnhàcungcấp
“supplierCode”:string, //Mãnhàcungcấp
“partnerType”:string, //Ngườinộp/nhận
“purchaseById”:long?, //Idngườinhập
“purchaseName”:int, //Tênngườinhập
“purchaseOrderDetails”:[{
“productId”:long, //Idhànghóa
“productCode”:string, //Mãhànghóa
“productName”:string, //Tênhànghóa
“quantity”:double, //Sốlượnghànghóa
“price”:decimal, //Giánhập
CôngTyCổphầnCôngnghệKiotViet 140/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“discount”:decimal, //Giảmgiá
“discountRatio”:double?,//Giảmgiá
}]//Thôngtinnhậphàngchitiết
“payments”:[{
“id”:long, //Idthanhtoán
“code”:string, //Mãthanhtoán
“method”:string, //Phươngthứcthanhtoán,
“amount”:decimal, //Phươngthứcthanhtoán,
“status”:int, //Trạngthái
“statusValue”:string,//Têntrạngthái
“transDate”:DateTime,//NgàyThanhtoán
}]//Thôngtinthanhtoán
}
2.15.5. Xóanhậphàng
Mụcđíchsửdụng:XóađơnđặthàngtheoID
Phương thức và URL: DELETE
https://public.kiotapi.com/purchaseorders?id={Id}&IsVoidPayment=true
Request:GồmIdcủaphiếunhậphàngtrongURL:
“id”:long//IDcủaphiếunhậphàng
“IsVoidPayment”:bool//Hủyphiếuthanhtoán,nếukhôngtruyềnthamsốnàythìmặcđịnh
khônghủyphiếuthanhtoángắnkèmđặthàng
Response:
{
"message":"Xóadữliệuthànhcông"
CôngTyCổphầnCôngnghệKiotViet 141/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
2.16. Chuyểnhàng
2.16.1. Lấydanhsáchchuyểnhàng
Mụcđíchsửdụng:Trảvềdanhsáchphiếuchuyểnhàng.
PhươngthứcvàURL:GEThttps://public.kiotapi.com/transfers
Request:SửdụnghàmGETvớithamsố:
{
“toBranchIds”:int[],optional//IDschinhánhnhận
“fromBranchIds”:int[],optional//IDschinhánhchuyển
“status”:int[],optional//Tìnhtrạngphiếuchuyển
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int?,//LấydữliệutừbảnghicurrentItem,
“fromReceivedDate”:DateTime?,//Từthờigiannhậnchuyểnhàng,
“toReceivedDate”:DateTime?,//Đếnthờigiannhậnchuyểnhàng,
“fromTransferDate”:DateTime?,//Từthờigianchuyểnhàng,
“toTransferDate”:DateTime?,//Đếnthờigianchuyểnhàng,
}
Response:
{
“total”:int,
“pageSize”:int,
“data”:[{
“id”:long//Idphiếu
“code”:string//Mãphiếu
“fromBranchId”:int,//Idchinhánhchuyển
“toBranchId”:int,//Idchinhánhnhận
CôngTyCổphầnCôngnghệKiotViet 142/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“status”:int,//trạngtháiphiếuchuyển
“retailerId”:long,//Idgianhàng
“description”:string,//ghichú
“transferDetails”:[{
“productId”:long,//Idhànghóa
“ProductCode”:string,//mãhànghóa
“sendQuantity”:double,//Sốlượnghànghóachuyển
“recieveQuantity”:double,//Sốlượnghànghóanhận
“price”:decimal,//Giátrị
“sendPrice”:decimal,//giáchuyển
"receivePrice":decimal,//giánhận
}]//Thôngtinchuyểnhàngchitiết
}]
}
2.16.2. Lấychitiếtchuyểnhàng
Mụcđíchsửdụng:TrảlạichitiếtcủamộtphiếuchuyểnhàngcụthểtheoID
PhươngthứcvàURL:
TheoId:GEThttps://public.kiotapi.com/transfers/{id}
Request:SửdụnghàmGETvớithamsố:
“id”:long//IDcủaphiếuchuyểnhàng
Response:
{
"id":long,//IDphiếuchuyểnhàng
"code":string,//Mãphiếuchuyểnhàng
"status":int,//Trạngtháiphiếuchuyểnhàng
CôngTyCổphầnCôngnghệKiotViet 143/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"transferredDate":datetime,//Ngàychuyển
"receivedDate":datetime,//Ngàynhận
"createdById":long,//IDngườitạo
"createdByName":string,//Tênngườitạo
"fromBranchId":long,//IDchinhánhnguồn
"fromBranchName":string,//Tênchinhánhnguồn
"toBranchId":long,//IDchinhánhđích
"toBranchName":string,//Tênchinhánhđích
"noteBySource":string,//Ghichúnguồn
"noteByDestination":string,//ghichúđích
"details":[
{
"id":long,//IDchitiếtphiếuchuyểnhàng
"productId":long,//IDsảnphẩm
"productCode":string,//Mãsảnphẩm
"productName":string,//Tênsảnphẩm
"transferredQuantity":int,//Sốlượngsảnphẩm
"price":decimal,//Đơngiásảnphẩm
"totalTransfer":decimal,//Tổngtiềnchuyển
"totalReceive":decimal//Tổngtiềnnhận
}
]
}
2.16.3. Thêmmớichuyểnhàng
Mụcđíchsửdụng:Trảvềdanhsáchphiếuchuyểnhàng.
CôngTyCổphầnCôngnghệKiotViet 144/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
PhươngthứcvàURL:POSThttps://public.kiotapi.com/transfers
Requestbody:
{
"fromBranchId":long, //idchinhánhchuyển
"toBranchId":long, //idchinhánhnhận
"isDraft":int, //idchinhánhnhận
"code":string, //idchinhánhnhận
"description":string, //idchinhánhnhận
"status":int, //idchinhánhnhận
"transferDetails":[
{
"productCode":string, //mãhànghóa
"productId":long, //idhànghóa
"sendQuantity":double, //sốlượngchuyển
"recivedQuantity":double, //sốlượngnhận
"price":decimal //giáchuyển
}
] //danhsáchchitiếtphiếuchuyển
}
Response:
{
"message":string,//thôngbáothànhcông
"data":{
"code":string, //Mãphiếuchuyển
"description":string, //Nộidungghichú
CôngTyCổphầnCôngnghệKiotViet 145/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"dispatchedDate":datetime, //Ngàychuyển
"fromBranchId":long, //idchinhánhchuyển
"id":long, //idphiếuchuyển
"isActive":bool, //trạngthái
"retailerId":long, //idretailer
"status":int, //trạngtháiphiếuchuyển
"toBranchId":long, //idchinhánhchuyển
"transferDetails":[
{
"productCode":string, //mãhànghóa
"productId":long, //idhànghóa
"sendQuantity":double, //sốlượngchuyển
"recivedQuantity":double, //sốlượngnhận
"sendPrice":decimal, //giáchuyển
"receivePrice":decimal, //giánhận
"price":decimal
}
]
}
}
2.16.4. Cậpnhậtchuyểnhàng
Mụcđíchsửdụng:CậpnhậtphiếuchuyểnhàngtheoID
PhươngthứcvàURL:PUThttps://public.kiotapi.com/transfers/id
Request:SửdụnghàmPUTvớiIDphiếuchuyểnhàngqua1objectJSON.
“id”:long//IDphiếuchuyểnhàng
CôngTyCổphầnCôngnghệKiotViet 146/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Body
{
"fromBranchId":long,//IDchinhánhnguồn
"toBranchId":long,//IDchinhánhđích
"isDraft":boolean, //Tạodạngphiếutạm
"code":string,//Mãphiếuchuyển
"description":string,//Môtả
"status":int,//Trạngtháiphiếuchuyển
"transferDetails":[
{
"transferId":long,//IDphiếuchuyển
"productCode":string, //Mãsảnphẩm
"productId":long, //IDsảnphẩm
"sendQuantity":double,//Sốlượnggửi
"recivedQuantity":double,//Sốlượngnhận
"price":decimal//Đơngiá
}
]
}
Response:
{
"message":string,//Thôngbáotrạngtháitrảvề,thànhcông/thấtbại
"data":{
"code":string,//Mãphiếuchuyển
CôngTyCổphầnCôngnghệKiotViet 147/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"description":string,//Môtả
"dispatchedDate":datetime,//Ngàytạo
"fromBranchId":long,//IDchinhánhnguồn
"id":long,//IDphiếuchuyển
"isActive":boolean,//đanghoạtđộng?
"retailerId":long,//IDgianhàng
"status":int,//Trạngthái
"toBranchId":long,//IDchinhánhđích
"transferDetails":[
{
"productId":long,//IDsảnphẩm
"productCode":string,//Mãsảnphẩm
"sendQuantity":double,//Sốlượnggửi
"receiveQuantity":double,//Sốlượngnhận
"sendPrice":decimal,//Giágửi
"receivePrice":decimal,//Giánhận
"price":decimal,//Đơngiá
"transferId":long//IDphiếuchuyển
}
]
}
}
2.16.5. Xóaphiếuchuyểnhàng
Mụcđíchsửdụng:XóaphiếuchuyểnhàngtheoID
PhươngthứcvàURL:DELETEhttps://public.kiotapi.com/transfers/{id}
CôngTyCổphầnCôngnghệKiotViet 148/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Request:GồmIdcủaphiếuchuyểnhàng:
“id”:long//IDcủaphiếuchuyểnhàng
Response:Trảlạithôngtinxóathànhcông(Code200)
{
"message":"Xóadữliệuthànhcông"
}
2.17. Bảnggiá
2.17.1. Lấydanhsáchbảnggiá
Mụcđíchsửdụng:Trảvềdanhsáchbảnggiá
PhươngthứcvàURL:GEThttps://public.kiotapi.com/pricebooks
Request:SửdụnghàmGETvớithamsố:
- “includePriceBookBranch”:Boolean,optional//Cólấythôngtindanhsáchchinhánháp
dụngbảnggiá
- “includePriceBookCustomerGroups”: Boolean, optional // Có lấy thông tin danh sách
nhómKHápdụngbảnggiá
- “includePriceBookUsers”:Boolean,optional//Cólấythôngtindanhsáchngườidùngáp
dụngbảnggiá
- “orderBy”:string,//SắpxếpdữliệutheotrườngorderBy(Vídụ:orderBy=name)
- “orderDirection”:string,//Sắpxếpkếtquảtrảvềtheo:TăngdầnAsc(Mặcđịnh),giảm
dầnDesc
- “currentItem”:int?,
- “pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
- “lastModifiedFrom”:datetime?//thờigiancậpnhật
Response:
{
“total”:int,tổng
“pageSize”:int,baonhiêudòng/1trangdữliệu
CôngTyCổphầnCôngnghệKiotViet 149/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“data”:[{
“id”:long//idbảnggiá
“name”:string//tênbảnggiá
“isActive”:boolean//trạngtháihoạtđộnghaykhông
“isGlobal”:boolean,//cóphảilàbảnggiáchungkhông
“startDate”:datetime,//ngàybắtđầuápdụng
“endDate”:datetime,//ngàyhếthạn
“forAllCusGroup”:boolean//ápdụngchotấtcảnhómkháchhàng
“forAllUser”:boolean,//ápdụngchotấtcảuser
“priceBookBranches”://
[{
“id”:long,//Idquanhệbảnggiá–chinhánh
“priceBookId”:long,//IDbảnggiá
“branchId”:long,//IDchinhánhápdụng
}],
“priceBookCustomerGroups”://
[{
“customerGroupName”:string,
“id”:long,//Idquanhệbảnggiá–nhómkháchhàng
“priceBookId”:long,//IDbảnggiá
“customerGroupId”:long,//IDnhómkháchhàng
}],
“priceBookUsers”:[{
“userName”:string,//Tênngườidùng
“id”:long,//Idquanhệ
“priceBookId”:long,//IDbảnggiá
“userId”:long,//IDngườidùng
CôngTyCổphầnCôngnghệKiotViet 150/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}],
}]
}
2.17.2. Lấychitiếtbảnggiá
Mụcđíchsửdụng:TrảvềthôngtinchitiếtcủabảnggiátheoID
PhươngthứcvàURL:
- TheoId:GEThttps://public.kiotapi.com/pricebooks/{id}
Request:SửdụnghàmGETvớithamsố:
“id”:long//IDcủabảnggiá
“orderBy”:string,//SắpxếpdữliệutheotrườngorderBy(Vídụ:orderBy=name)
“orderDirection”: string, //Sắp xếp kết quả trả về theo: Tăng dần Asc (Mặc định),
giảmdầnDesc
“currentItem”:int?//lấydữliệutừbảnghihiệntại,nếukhôngnhậpthìmặcđịnhlà
0
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“lastModifiedFrom”:datetime?//thờigiancậpnhật
Response:
{
“total”:int,
“pageSize”:int,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“data”:[{
“productId”:long//IDcủahànghóa
“productCode”:string//codecủahànghóa
“price”:decimal//giá
CôngTyCổphầnCôngnghệKiotViet 151/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}]}
2.17.3. Cậpnhậtchitiếtbảnggiá
Mụcđíchsửdụng:Cậpnhậtthôngtingiábáncủahànghóatrongbảnggiá
PhươngthứcvàURL:POSThttps://public.kiotapi.com/pricebooks/detail
Request:SửdụnghàmPOSTvớithamsố:
{
“pricebookId”:long,//IDcủabảnggiá,mặcđịnhlà0(Bảnggiáchung)
“productId”:long,//IDcủahànghóa
“price”:decimal//Giácủahànghóa
}
Response:
{
"message":string,//Nộidungthôngbáo
"isSuccess":boolean//Thànhcôngkhông
}
2.18. Kênhbánhàng
2.18.1. Lấydanhsáchkênhbánhàng
Mụcđíchsửdụng:Trảvềdanhsáchhóađơntheocửahàngđãđượcxácnhận
PhươngthứcvàURL:GEThttps://public.kiotapi.com/salechannel
Request:SửdụnghàmGETvớithamsố:
“orderBy”:string,//SắpxếpdữliệutheotrườngorderBy
(Vídụ:orderBy=name)
“orderDirection”: string, //Sắp xếp kết quả trả về theo: Tăng dần Asc (Mặc định),
giảmdầnDesc
CôngTyCổphầnCôngnghệKiotViet 152/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“currentItem”:int?//lấydữliệutừbảnghihiệntại,nếukhôngnhậpthìmặcđịnhlà
0
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“lastModifiedFrom”:datetime?//thờigiancậpnhật
Response:
{
“total”:int,
“pageSize”:int,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“data”:[{
“id”:long//Idkênhbánhàng
“name”:string//tênkênhbánhàng
“isActive”:boolean//cònsửdụngkhông
“img”:string//đườngdẫnảnhđạidiện
“isNotDelete”:boolean//true=khôngthểxóa
}]}
2.19. Trảhàng
2.19.1. Lấydanhsáchtrảhàng
Mụcđíchsửdụng:Trảvềdanhsáchtrảhàngtheocửahàngđãđượcxácnhận
PhươngthứcvàURL:GEThttps://public.kiotapi.com/returns
Request:SửdụnghàmGETvớithamsố:
“orderBy”: string, optional //Sắp xếp dữ liệu theo trường orderBy (ví dụ:
orderBy=Name)
“lastModifiedFrom”:datetime?//thờigiancậpnhật
"fromReturnDate":"date",optinal//từngàytrảhàng
CôngTyCổphầnCôngnghệKiotViet 153/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"toReturnDate":"date",optinal//đếnngàytrảhàng
“pageSize”:int,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int,//lấydữliệutừbảnghicurrentItem
“includePayment”:Boolean,//cólấythôngtindanhsáchthanhtoán?
“orderDirection”:string,optional
Nếucó"OrderDirection",chọnsắpxếpkếtquảvềtheo:
 ASC(Mặcđịnh)
 DESC
Response:
{
“total”:int,
“pageSize”:int,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“data”:[{
“id”:long//Idtrảhàng
“code”:string//Mãtrảhàng
“invoiceId”:long?//Idhóađơn
“returnDate”:datetime//Ngàytrảhàng
“branchId”:int,//Idchinhánh
“branchName”:string,//Tênchinhánh
“receivedById”:long//Idngườinhậntrả
“soldByName”:string//Tênngườibánhàng
“customerId”:long?,//Idkháchhàng
“customerCode”:string,Mãkháchhàng
“customerName”:string,//Tênkháchhàng
“returnTotal”:decimal,//Tổngtiềntrảhàng
“totalPayment”:decimal,//Tổngtiềnkháchtrả
CôngTyCổphầnCôngnghệKiotViet 154/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“status”:int,//trạngtháiphiếutrảhàng
“statusValue”:string,//trạngtháiđơntrảhàngbằngchữ
“createdDate”:datetime,//Ngàytạo
“modifiedDate”:datetime,//Ngàycậpnhật
“payments”:[{
“id”:long,
“code”:string,
“amount”:decimal,
“method”:string”,
“status”:byte?,
“statusValue”:string,
“transDate”:datetime,
“bankAccount”:string,
“accountId”:int?,
“description”:string
}],
“returnDetails”:[{
“productId”:long,//Idhànghóa
“productCode”:string,//Mãhànghóa
“productName”:string,//Tênhànghóa
(baogồmthuộctínhvàđơnvịtính)
“quantity”:double,//Sốlượnghànghóa
“price”:decimal,//Giátrị
“note”:string//Ghichúhànghóa
“usePoint”:bool?//Códùngtíchđiểmhaykhông
“subTotal”:decimal//Tổngtiềnhàng
CôngTyCổphầnCôngnghệKiotViet 155/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}]
}]}
2.19.2. Lấychitiếtphiếutrảhàng
Mụcđíchsửdụng:TrảvềthôngtinchitiếtcủaphiếutrảhàngtheoID,theoCode
PhươngthứcvàURL:
- TheoId:GEThttps://public.kiotapi.com/returns/{id}
- TheoCode:GEThttps://public.kiotapi.com/returns/code/{code}
Request:SửdụnghàmGETvớithamsố:
“id”:long//IDcủatrảhàng
“code”:string//Mãcủatrảhàng
Response:
{
“id”:long//Idtrảhàng
“code”:string//Mãtrảhàng
“invoiceId”:long?//Idhóađơn
“returnDate”:datetime//Ngàytrảhàng
“branchId”:int,//Idchinhánh
“branchName”:string,//Tênchinhánh
“receivedById”:long//Idngườinhậntrả
“soldByName”:string//Tênngườibánhàng
“customerId”:long?,//Idkháchhàng
“customerCode”:string,Mãkháchhàng
“customerName”:string,//Tênkháchhàng
“returnTotal”:decimal,//Tổngtiềntrảhàng
CôngTyCổphầnCôngnghệKiotViet 156/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“returnDiscount”:decimal?,//Giảmgiátrảhàng
“returnFee”:decimal?,//Phítrảhàng
“totalPayment”:decimal,//Tổngtiềnkháchtrả
“status”:int,//trạngtháiphiếutrảhàng
“statusValue”:string,//trạngtháiđơnđặthàngbằngchữ
“createdDate”:datetime,//Ngàytạo
“modifiedDate”:datetime,//Ngàycậpnhật
“payments”:[{
“id”:long,
“code”:string,
“amount”:decimal,
“method”:string”,
“status”:byte?,
“statusValue”:string,
“transDate”:datetime,
“bankAccount”:string,
“accountId”:int?,
“description”:string
}],
“returnDetails”:[{
“productId”:long,//Idhànghóa
“productCode”:string,//Mãhànghóa
“productName”:string,//Tênhànghóa
(baogồmthuộctínhvàđơnvịtính)
“quantity”:double,//Sốlượnghànghóa
“price”:decimal,//Giátrị
CôngTyCổphầnCôngnghệKiotViet 157/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“note”:string//Ghichúhànghóa
“usePoint”:bool?//Códùngtíchđiểmhaykhông
“subTotal”:decimal//Tổngtiềnhàng
}]
}
2.20. Đặthàngnhập
2.20.1. Lấydanhsáchđặthàngnhập
Mụcđíchsửdụng:Trảvềdanhsáchđặthàngnhập
PhươngthứcvàURL:GEThttps://public.kiotapi.com/ordersuppliers
Request:SửdụnghàmGETvớithamsố:
“branchId”:int?//Idchinhánh
“status”:int?//Trạngtháiđặthàngnhập
“productKey”:string,//Mãnhậphàng
“supplierKey”:string,//Mãnhàcungcấp
“userNamKey”:string,//Mãngườitạo
“userNamCreatedKey”:string,//Mãngườiđặt
“expensesOthersIds”:string,//Chiphínhậptrảnhàcungcấp
“descriptionKey”:string,//Ghichú
“codeKey”:string,//Mãphiếuđặthàngnhập
“purchaseOrderCode”:string,//Mãphiếunhậphàng
Response:
{
“total”:int,
“pageSize”:int,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“data”:[{
CôngTyCổphầnCôngnghệKiotViet 158/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“id”:long,//Idnhậphàng
“code”:string,//Mãđặthàng
“invoiceId”:long?,//Idnhậphàng
“orderDate”:datetime,//Ngàyđặt
“branchId”:int,//Idchinhánh
“retailerId”:int,//Idcửahàng
“userId”:long,//Idngườidùng
“description”:string,//Ghichú
“status”:int,//Trạngthái
“discountRatio”:string,//Giảmgiátheo%
“productQty”:double?,//Sốlương
“discount”:decimal?,//Giảmgiá
“createdDate”:datetime,//Ngàytạo
“createdBy”:long,//Idngườitạo
“orderSupplierDetails”:[
{
“id”:long,
“orderSupplierId”:long,
“productId”:long,
“quantity”:double,
“price”:decimal,
“discount”:decimal,
“allocation”:decimal,
“createdDate”:datime,
“description”:string,
“orderByNumber”:int?,
CôngTyCổphầnCôngnghệKiotViet 159/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“allocationSuppliers”:decimal?,
“allocationThirdParty”:decimal?,
“orderQuantity”:double,
“subTotal”:decimal,
}
],
“OrderSupplierExpensesOthers”:[
{
“id”:long,
“form”:int?,
“expensesOtherOrder”:byte?,
“expensesOtherCode”:string,
“expensesOtherName”:string,
“expensesOtherId”:int,
“orderSupplierId”:long?,
“price”:decimal,
“isReturnAuto”:bool?,
“exValue”:decimal?,
“createdDate”:datetime
}
],
“total”:decimal,
“exReturnSuppliers”:decimal?,
“exReturnThirdParty”:decimal?,
“totalAmt”:decimal?,
CôngTyCổphầnCôngnghệKiotViet 160/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“totalQty”:double?,
“totalQuantity”:double,
“subTotal”:decimal,
“paidAmount”:decimal,
“toComplete”:bool,
“statusValue”:string,
“viewPrice”:bool,
“supplierDebt”:decimal,
“supplierOldDebt”:decimal,
“purchaseOrderCodes”:string,
}]}
2.20.2. Lấychitiếtđặthàngnhập
Mụcđíchsửdụng:Trảvềthôngtinchitiếtcủaphiếuđặthàngnhập
PhươngthứcvàURL:
GEThttps://public.kiotapi.com/ordersuppliers/{id}
Request:SửdụnghàmGETvớithamsố:
“id”:long//IDđặthàngnhập
Response:
{
“total”:int,
“pageSize”:int,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“data”:[{
“id”:long,//Idnhậphàng
CôngTyCổphầnCôngnghệKiotViet 161/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“code”:string,//Mãđặthàng
“invoiceId”:long?,//Idnhậphàng
“orderDate”:datetime,//Ngàyđặt
“branchId”:int,//Idchinhánh
“retailerId”:int,//Idcửahàng
“userId”:long,//Idngườidùng
“description”:string,//Ghichú
“status”:int,//Trạngthái
“discountRatio”:string,//Giảmgiátheo%
“productQty”:double?,//Sốlương
“discount”:decimal?,//Giảmgiá
“createdDate”:datetime,//Ngàytạo
“createdBy”:long,//Idngườitạo
“orderSupplierDetails”:[
{
“id”:long,
“orderSupplierId”:long,
“productId”:long,
“quantity”:double,
“price”:decimal,
“discount”:decimal,
“allocation”:decimal,
“createdDate”:datime,
“description”:string,
“orderByNumber”:int?,
“allocationSuppliers”:decimal?,
CôngTyCổphầnCôngnghệKiotViet 162/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“allocationThirdParty”:decimal?,
“orderQuantity”:double,
“subTotal”:decimal,
}
],
“OrderSupplierExpensesOthers”:[
{
“id”:long,
“form”:int?,
“expensesOtherOrder”:byte?,
“expensesOtherCode”:string,
“expensesOtherName”:string,
“expensesOtherId”:int,
“orderSupplierId”:long?,
“price”:decimal,
“isReturnAuto”:bool?,
“exValue”:decimal?,
“createdDate”:datetime
}
],
“total”:decimal,
“exReturnSuppliers”:decimal?,
“exReturnThirdParty”:decimal?,
“totalAmt”:decimal?,
“totalQty”:double?,
CôngTyCổphầnCôngnghệKiotViet 163/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
“totalQuantity”:double,
“subTotal”:decimal,
“paidAmount”:decimal,
“toComplete”:bool,
“statusValue”:string,
“viewPrice”:bool,
“supplierDebt”:decimal,
“supplierOldDebt”:decimal,
“purchaseOrderCodes”:string,
}]}
2.21. Lấydanhsáchlocation
Mụcđíchsửdụng:Trảvềthôngtinlocation
PhươngthứcvàURL:
GEThttps://public.kiotapi.com/locations
Request:Khôngcóthamsố
Response:
{
“total”:int,
“pageSize”:int,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“data”:[{
“id”:long,//Idlocation
“name”:string,//Tênlocation
“normalName":string//Tênkhôngdấu
CôngTyCổphầnCôngnghệKiotViet 164/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}]
}
2.22. Thiếtlậpcửahàng
Mụcđíchsửdụng:Trảvềdanhsáchthiếtlậpcửahàng
PhươngthứcvàURL:GEThttps://public.kiotapi.com/settings
Request:SửdụnghàmGET
Response:
{
“ManagerCustomerByBranch”:bool,//Quảnlíkháchhàngtheochinhánh
“AllowOrderWhenOutStock”:bool,//Chophépđặthàngkhihếttồnkho
“AllowSellWhenOrderOutStock”:bool,//Bánhàng,chuyểnhàngkhisảnphẩmđãđược
đặthàng
“AllowSellWhenOutStock“:bool//Bánhàng,Chuyểnhàng,Trảhàngnhập,Sảnxuất,Xuất
hủykhihếttồnkho
}
2.23. CậpnhậttrạngtháiCoupon
Mụcđíchsửdụng:CậpnhậttrạngtháimãCoupon về"Đãsửdụng"
PhươngthứcvàURL:POSThttps://public.kiotapi.com/coupons/setused
Request:SửdụnghàmPOST
Body:
{
"coupons":[{
"code":"string" //Mãcoupon:require
}]
CôngTyCổphầnCôngnghệKiotViet 165/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
Response:
{
"message":string,//nộidungthôngbáo
"dataError":[{
"code":"string"//thôngbáolỗitươngứng
}]
}
2.24. Voucher
2.24.1. Lấydanhsáchđợtpháthành
Mụcđíchsửdụng:Trảvềdanhsáchđợtpháthànhvouchervàchitiếtđợtpháthành
PhươngthứcvàURL:GEThttps://public.kiotapi.com/vouchercampaign
Request:SửdụnghàmGETvớithamsố:
“includeVoucherBranchs”: Boolean, optional//có lấy thông tin danh sách chi nhánh
ápdụngvoucher
“includeVoucherUsers”: Boolean, optional //có lấy thông tin danh sách người tạo
ápdụngvoucher
"isActive":Boolean,optional//trạngtháiđợtpháthành
"id":long,optional//idđợtpháthành
"isGlobal":boolean,optional//cóápdụngchotoànhệthống
"forAllCusGroup":boolen,optional//cóápdụngchotoànbộkháchhàng
"forAllUser":boolean,optional//cóápdụngchotoànbộngườitạo
Response:
{
“total":int,//tổng
CôngTyCổphầnCôngnghệKiotViet 166/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"data":[{
"id":long,//idđợtpháthành
"code":string,//mãđợtpháthành
"name":string,//tênđợtpháthành
"isActive":boolean,//trạngtháiđợtpháthành
"startDate":datetime,//thờigianápdụngbắtđầu
"endDate":datetime,//thờigianápdụngkếtthúc
"expireTime":long,//sốngàykểtừngàypháthànhsẽhếthạnsửdụngvoucher
"prereqCategoryIds":int[],//danhsáchidnhómhàng
"prereqProductIds":long[],//danhsáchidhànghóa
"prereqPrice":decimal,//tổngtiềnhàng
"quantity":int,//tổngsốvoucher
"price":decimal,//mệnhgiá
"useVoucherCombineInvoice":boolean,//ápdụnggộpnhiềuvouchertrên1hóađơn
"isGlobal":boolean,//cóápdụngchotoànhệthống
"forAllCusGroup":boolen,//cóápdụngchotoànbộkháchhàng
"forAllUser":boolean,//cóápdụngchotoànbộngườitạo
"voucherBranchs":[{
"branchId":int,//idchinhánh
"branchName":string,//tênchinhánh
}],
"voucherUsers":[{
"userId":int,//idngườitạo
"userName":string,//tênngườitạo
}]
}]
CôngTyCổphầnCôngnghệKiotViet 167/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
2.24.2. Lấydanhsáchvouchertrongđợtpháthành
Mụcđíchsửdụng:Trảvềdanhsáchvouchertrongđợtpháthành
PhươngthứcvàURL:GEThttps://public.kiotapi.com/voucher
Request:SửdụnghàmGETvớithamsố:
"campaignId":long,//idđợtpháthànhvoucher
"status": int, optional //trạng thái = [0: chưa sử dụng | 1: đã phát hành | 2: đã sử
dụng|3:đãhủy]
Response:
{
"total":int,//tổng
"data":[{
"id":long,//idvoucher
"code":string,//mãvoucher
"voucherCampaignId":long,//idđợtpháthànhvoucher
"releaseDate":datetime,//ngàypháthành
"expireDate":datetime,//ngàyhếthạn
"usedDate":datetime,//ngàysửdụng
"status":int,//trạngthái=[0:chưasửdụng|1:đãpháthành|2:đãsửdụng|3:đãhủy]
"sellType":int,//hìnhthức=[0:tặng|1:bán]
"price":decimal,//giátrịvoucher
"partnerType":string,//nhómngườimuanhậnvoucher=[U:nhânviên|C:kháchhàng|S:
nhânviên|O:khác|D:Đốitácgiaohàng]
"partnerId":long,//idngườimuanhậnvoucher
"partnerName":string,//tênngườimuanhậnvoucher }]
CôngTyCổphầnCôngnghệKiotViet 168/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
2.24.3. Tạomớivoucher
Mụcđíchsửdụng:Tạomớivouchertheođợtpháthành
PhươngthứcvàURL:POSThttps://public.kiotapi.com/voucher
Request:SửdụnghàmPOSTvớithamsố:
{
"voucherCampaignId":long,//idđợtpháthànhvoucherđangởtrạngtháikíchhoạt
"data":[{
"code":string,//mãvoucher
}]
}
Response:
{
"message":"Thêmmớivoucherthànhcông"
}
2.24.4. Pháthànhvoucher
Mụcđíchsửdụng:Pháthànhvouchertheođợtpháthành(theohìnhthứctặng)
PhươngthứcvàURL:POSThttps://public.kiotapi.com/voucher/release/give
Request:SửdụnghàmPOSTvớithamsố:
{
"CampaignId":long,//idđợtpháthànhvoucherđangởtrạngtháikíchhoạt
"Vouchers":[
{
"Code":string,//mãvoucher
CôngTyCổphầnCôngnghệKiotViet 169/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
}
],//chỉápdụngvớivoucherđangởtrạngtháilà0:chưasửdụng
"ReleaseDate":datetime,//ngàypháthành
}
Response:
{
"message":"Cậpnhậtvoucherthànhcông"
}
2.24.5. Hủyvoucher
Mụcđíchsửdụng:Hủyvouchertheođợtpháthành
PhươngthứcvàURL:POSThttps://public.kiotapi.com/voucher/cancel
Request:SửdụnghàmPOSTvớithamsố:
{
"CampaignId":long,//idđợtpháthànhvoucherđangởtrạngtháikíchhoạt
"Vouchers":[
{
"Code":string,//mãvoucher
}],//chỉápdụngvớivoucherđangởtrạngtháilà0:chưasửdụng
}
Response:
{
"message":"Cậpnhậtvoucherthànhcông"
}
CôngTyCổphầnCôngnghệKiotViet 170/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
2.25. Thươnghiệu
2.25.1. Lấydanhsáchthươnghiệu
Mụcđíchsửdụng:Trảvềtoànbộdanhmụcthươnghiệucủahànghóa.Danhsáchnàyđược
sắpxếptheothứtựbảngchữcái(a-z).
PhươngthứcvàURL:GEThttps://public.kiotapi.com/trademark
Request:SửdụnghàmGETvớithamsố:
{
“lastModifiedFrom”:datetime?//thờigiancậpnhật
“pageSize”:int?,//sốitemstrong1trang,mặcđịnh20items,tốiđa100items
“currentItem”:int,//lấydữliệutừbảnghihiệntại,nếukhôngnhậpthìmặcđịnhlà0
“orderBy”:string,//SắpxếpdữliệutheotrườngorderBy(Vídụ:orderBy=name)
“orderDirection”:string,//Sắpxếpkếtquảtrảvềtheo:TăngdầnAsc(Mặcđịnh),giảmdần
Desc
}
Response:
{
“total":int,//tổng
“pageSize”:int,
“data":[{
“tradeMarkId”:int,//IDthươnghiệu
“tradeMarkName”:int,//IDthươnghiệu
“createdDate”:datetime,//thờigiantạothươnghiệu
“modifiedDate”:datetime,//thờigiancậpnhậtthươnghiệu,nếuchưatừng
cậpnhậtthìbằngthờigiantạo}]
}
2.26. Nhàcungcấp
2.26.1. Lấydanhsáchnhàcungcấp
Mụcđíchsửdụng:Trảvềdanhsáchnhàcungcấpcủacửahàng
CôngTyCổphầnCôngnghệKiotViet 171/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
Phươngthức:GET
URL:https://public.kiotapi.com/suppliers
Request:SửdụnghàmGETvớithamsố:
{
"pageSize":int?,//Sốbảnghitrong1trang,mặcđịnh20bảnghi,tốiđa100bảnghi
"currentItem":int?,//Vịtríbắtđầulấydữliệu,mặcđịnhlấytừbảnghisố1
"orderDirection":string?,//Sắpxếpkếtquảtrảvề(Asc:tăngdần,mặcđịnh|Desc:giảm
dần)
"code":string?,//Tìmkiếmtheomãnhàcungcấp
"name":string?,//Tìmkiếmtheotênnhàcungcấp
"contactNumber":string?,//Tìmkiếmtheođiệnthoạinhàcungcấp
"lastModifiedFrom":datetime?,//Tìmkiếmtheokhoảngthờigiancậpnhật
"includeRemoveIds":boolean?,//CólấythôngtindanhsáchIDnhàcungcấpbịxóadựa
trênmodifiedDate
"includeTotal":boolean?,//CólấythôngtintotalInvoiced,totalInvoicedWithoutReturn
"includeSupplierGroup":boolean?//CólấythôngtinGroups
}
Response:
{
"removedId":int[],//DanhsáchIDnhàcungcấpbịxóadựatrênmodifiedDate
"total":int,//Tổngsốnhàcungcấp
"pageSize":int,//Sốbảnghitrong1trang,mặcđịnh20bảnghi,tốiđa100bảnghi
"data":[
{
"id":long,//IDnhàcungcấp
CôngTyCổphầnCôngnghệKiotViet 172/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"code":string,//Mãnhàcungcấp
"name":string,//Tênnhàcungcấp
"contactNumber":string,//Điệnthoại
"email":string,//Email
"address":string,//Địachỉ
"locationName":string,//Khuvực
"wardName":string,//Phườngxã
"organization":strting,//Têncôngty
"taxCode":string,//Mãsốthuế
"comments":string,//Ghichú
"groups":"string",//Danhsáchnhómnhàcungcấpngăncáchbởidấuphẩy
"isActive":boolean,//Trạngtháihoạtđộng(true:đanghoạtđộng|false:ngừnghoạt
động)
"modifiedDate":datetime,//Thờigiancậpnhậtthôngtinnhàcungcấpgầnnhất
"createdDate":datetime,//Thờigiantạonhàcungcấp
"retailerId":long,//IDgianhàng
"branchId":long,//IDchinhánhtạonhàcungcấp
"createdBy":string,//Tênngườitạonhàcungcấp
"debt":decimal,//Nợcầntrảhiệntại
"totalInvoiced":decimal,//Tổngmua
"totalInvoicedWithoutReturn":decimal//Tổngmuatrừtrảhàng
}
]
}
CôngTyCổphầnCôngnghệKiotViet 173/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
2.26.2. Lấychitiếtnhàcungcấp
Mục đích sử dụng: Trả về thông tin chi tiết của 1 nhà cung cấp theo ID nhà cung cấp hoặc
theomãnhàcungcấp(code)
Phươngthức:GET
URL:
-TheoID:https://public.kiotapi.com/suppliers/{id}
-Theocode:https://public.kiotapi.com/suppliers/{code}
Request:SửdụnghàmGETvớithamsố:
"id":long//IDcủanhàcungcấp
"code":string//Mãcủanhàcungcấp
Response:
{
"id":long,//IDnhàcungcấp
"code":string,//Mãnhàcungcấp
"name":string,//Tênnhàcungcấp
"contactNumber":string,//Điệnthoại
"email":string,//Email
"address":string,//Địachỉ
"locationName":string,//Khuvực
"wardName":string,//Phườngxã
"organization":strting,//Têncôngty
"taxCode":string,//Mãsốthuế
"comments":string,//Ghichú
"groups":"string",//Danhsáchnhómnhàcungcấpngăncáchbởidấuphẩy
"isActive":boolean,//Trạngtháihoạtđộng(true:đanghoạtđộng|false:ngừnghoạtđộng)
CôngTyCổphầnCôngnghệKiotViet 174/175

TàiliệuhướngdẫnsửdụngPublicAPI Ver4.7.1
"modifiedDate":datetime,//Thờigiancậpnhậtthôngtinnhàcungcấpgầnnhất
"createdDate":datetime,//Thờigiantạonhàcungcấp
"retailerId":long,//IDgianhàng
"branchId":long,//IDchinhánhtạonhàcungcấp
"createdBy":string,//Tênngườitạonhàcungcấp
"debt":decimal,//Nợcầntrảhiệntại
"totalInvoiced":decimal,//Tổngmua
"totalInvoicedWithoutReturn":decimal//Tổngmuatrừtrảhàng
}
CôngTyCổphầnCôngnghệKiotViet 175/175
