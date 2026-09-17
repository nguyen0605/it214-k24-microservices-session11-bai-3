# BÁO CÁO PHÂN TÍCH KIẾN TRÚC VÀ GIẢI PHÁP KỸ THUẬT
## CƠ CHẾ FAN-OUT VÀ LẮNG NGHE SỰ KIỆN (EVENT CONSUMER) TRONG KAFKA

---

### 1. Sơ đồ Kiến trúc Hệ thống (Broker Topology - Choreography)

Dưới đây là sơ đồ kiến trúc hoạt động của hệ thống khi có sự kiện `order.created` được đẩy vào Kafka topic `storex-order-events`. Hệ thống áp dụng mô hình Fan-out để đồng thời gửi sự kiện đến hai dịch vụ độc lập: **Inventory-Service** (nhóm 3 instances) và **Loyalty-Service** (nhóm 1 instance).

```mermaid
graph TD
    Producer[Order Service / Producer] -->|Publish order.created| Topic(Topic: storex-order-events)
    
    subgraph Topic: storex-order-events
        P0[Partition 0]
        P1[Partition 1]
        P2[Partition 2]
    end
    
    subgraph Consumer Group: inventory-group (Inventory-Service)
        Inv1[Inventory Instance 1]
        Inv2[Inventory Instance 2]
        Inv3[Inventory Instance 3]
    end
    
    subgraph Consumer Group: loyalty-group (Loyalty-Service)
        Loy1[Loyalty Instance 1]
    end
    
    P0 -->|Assign| Inv1
    P1 -->|Assign| Inv2
    P2 -->|Assign| Inv3
    
    P0 -->|Fan-out| Loy1
    P1 -->|Fan-out| Loy1
    P2 -->|Fan-out| Loy1
```

---

### 2. Phân tích & Giải quyết Sự cố BUG-04

#### Hiện tượng lỗi
Khi lập trình viên cấu hình nhầm chung một `group-id="storex-system"` cho cả hai ứng dụng **Inventory-Service** và **Loyalty-Service**, xảy ra tình trạng: Một số đơn hàng chỉ được trừ kho mà không được cộng điểm thưởng, và ngược lại, một số đơn hàng được cộng điểm thưởng nhưng kho không thay đổi.

#### Nguyên nhân gốc rễ
- **Cơ chế hoạt động của Consumer Group trong Kafka:** Kafka phân phối các message trong một Topic đến các Consumer trong cùng một Consumer Group theo cơ chế **Competing Consumers** (Cạnh tranh). 
- Một partition cụ thể của một topic tại một thời điểm chỉ được gán (assign) cho **duy nhất một consumer instance** trong một Consumer Group.
- Do đó, khi cấu hình chung `group-id="storex-system"`, Kafka coi toàn bộ các instance của cả 2 dịch vụ là một nhóm duy nhất. Khi một sự kiện `order.created` rơi vào một partition, nó sẽ chỉ được gửi đến một consumer duy nhất của nhóm (ví dụ: hoặc gửi tới Inventory-Service, hoặc gửi tới Loyalty-Service). Điều này triệt tiêu hoàn toàn tính chất **Fan-out** (Publish/Subscribe), dẫn tới việc mất mát logic nghiệp vụ song hành.

#### Giải pháp khắc phục
Tách biệt rõ ràng `group-id` cho từng dịch vụ để chúng hoạt động như hai thực thể Consumer Group độc lập:
- **Inventory-Service:** `group-id: inventory-group`
- **Loyalty-Service:** `group-id: loyalty-group`

Khi đó, mỗi nhóm nhận được toàn bộ 100% bản sao dữ liệu từ topic mà không hề can thiệp hay cạnh tranh quyền xử lý của nhau.

---

### 3. Thiết kế Số lượng Partition tối ưu (REQ-01)

#### Đề xuất số lượng Partition tối thiểu: **3 Partitions**

#### Lập luận chi tiết:
1. **Khả năng song song tối đa (Concurrency):** Trong Kafka, mức độ xử lý song song tối đa trong một Consumer Group bằng đúng số lượng Partitions của Topic đó. Với 3 instances của **Inventory-Service**, nếu cấu hình số lượng Partitions là 3, mỗi instance sẽ được gán chính xác 1 Partition để tiêu thụ dữ liệu độc lập, đạt hiệu năng tải tối đa ($33\%$ lượng đơn hàng cho mỗi server).
2. **Hệ quả nếu cấu hình không chuẩn:**
   - Nếu số Partitions **< 3** (ví dụ: 2 partitions): Sẽ có 1 instance của Inventory-Service luôn ở trạng thái nhàn rỗi (idle), lãng phí tài nguyên máy chủ.
   - Nếu số Partitions **> 3** (ví dụ: 6 partitions): Hệ thống chạy rất tốt, mỗi instance xử lý 2 partitions. Điều này còn giúp hệ thống dễ dàng mở rộng lên đến 6 instances khi ngày hội Mega Sale tăng tải đột biến mà không cần phân chia lại (repartition) Topic.
3. **Khuyến nghị thực tế:** Trong môi trường Production với lượng đơn hàng $10,000$ đơn/giây, nên thiết kế số lượng Partition là **6 hoặc 12 Partitions** kết hợp với khóa phân vùng (`orderId`) để vừa đảm bảo cân bằng tải, vừa đảm bảo tính tuần tự xử lý theo từng đơn hàng cụ thể.