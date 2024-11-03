import mailtrap as mt

def send_fraud_alert_email(
        customer_name: str,
        recipient_email: str,
        transaction_amount: str,
        transaction_time: str,
        support_phone: str = "1800-1234",
        support_email: str = "support@example.com",
        lock_card_link: str = "https://banking.example.com/lock_card"
) -> None:

    email_content = f"""
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Cảnh báo giao dịch</title>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
                font-family: 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif;
            }}
            body {{
                background: #f5f5f5;
                padding: 20px;
            }}
            .email-container {{
                max-width: 500px;
                margin: 0 auto;
                background: white;
                border-radius: 16px;
                overflow: hidden;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }}
            .header {{
                background: #ff3b30;
                color: white;
                padding: 20px;
                text-align: center;
            }}
            .header h1 {{
                font-size: 22px;
                margin-bottom: 10px;
            }}
            .header p {{
                font-size: 16px;
                opacity: 0.9;
            }}
            .content {{
                padding: 24px;
            }}
            .alert-box {{
                background: #fff5f5;
                border-left: 4px solid #ff3b30;
                padding: 16px;
                margin: 20px 0;
                border-radius: 0 8px 8px 0;
            }}
            .transaction-details {{
                background: #f8f9fa;
                padding: 16px;
                border-radius: 8px;
                margin: 20px 0;
            }}
            .transaction-details div {{
                display: flex;
                justify-content: space-between;
                margin: 8px 0;
                font-size: 15px;
            }}
            .action-button {{
                display: block;
                background: #ff3b30;
                color: white;
                text-decoration: none;
                padding: 16px 24px;
                border-radius: 8px;
                text-align: center;
                font-weight: bold;
                margin: 24px 0;
                transition: background 0.3s;
            }}
            .action-button:hover {{
                background: #d63027;
            }}
            .support-box {{
                background: #f8f9fa;
                padding: 16px;
                border-radius: 8px;
                margin-top: 24px;
                font-size: 14px;
                line-height: 1.6;
            }}
            .footer {{
                text-align: center;
                padding: 24px;
                color: #666;
                font-size: 14px;
                border-top: 1px solid #eee;
            }}
        </style>
    </head>
    <body>
        <div class="email-container">
            <div class="header">
                <h1>⚠️ Cảnh báo bảo mật</h1>
                <p>Phát hiện giao dịch đáng ngờ</p>
            </div>
            
            <div class="content">
                <p>Kính gửi <strong>{customer_name}</strong>,</p>
                
                <div class="alert-box">
                    Chúng tôi phát hiện một giao dịch bất thường trên thẻ của bạn. 
                    Vui lòng xác nhận nếu đây là giao dịch của bạn.
                </div>

                <div class="transaction-details">
                    <div>
                        <span>Thời gian:</span>
                        <strong>{transaction_time}</strong>
                    </div>
                    <div>
                        <span>Số tiền:</span>
                        <strong>{transaction_amount}</strong>
                    </div>
                </div>

                <a href="{lock_card_link}" class="action-button">
                    🔒 Khóa thẻ ngay lập tức
                </a>

                <div class="support-box">
                    <strong>Cần hỗ trợ?</strong><br>
                    Gọi ngay: {support_phone}<br>
                    Email: {support_email}
                </div>
            </div>

            <div class="footer">
                Đây là email tự động. Vui lòng không phản hồi email này.
            </div>
        </div>
    </body>
    </html>
    """

    mail = mt.Mail(
        sender=mt.Address(email="hello@demomailtrap.com", name="Bảo mật ngân hàng"),
        to=[mt.Address(email=recipient_email)],
        subject="🚨 Phát hiện giao dịch đáng ngờ - Xác nhận ngay",
        html=email_content,
        headers={
            "X-Priority": "1",
            "X-MSMail-Priority": "High",
            "Importance": "high"
        }
    )

    client = mt.MailtrapClient(token="d0e615a6c9cd288c46669c39ddcc80fb")
    client.send(mail)