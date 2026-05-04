LM Guild Manager - Stripe Billing Setup
=======================================

This update adds Stripe billing per guild with:
- £9.99 monthly plan
- £49.99 six-month plan
- £84.99 twelve-month plan
- 14-day trial via Stripe Checkout
- Site admin billing status view
- Payment event history
- Manual site-admin activation/bypass for your own guild or feature trials

1) Apply the update
-------------------
From your project folder:

python apply_stripe_billing_update.py

2) Stripe setup
---------------
In Stripe, create 3 recurring prices:

A) Monthly
Amount: £9.99
Recurring interval: monthly

B) 6 Months
Amount: £49.99
Recurring interval: every 6 months

C) 12 Months
Amount: £84.99
Recurring interval: yearly OR every 12 months

Copy the Price IDs into Render environment variables:

STRIPE_SECRET_KEY=sk_test_or_live_xxx
STRIPE_WEBHOOK_SECRET=whsec_xxx
STRIPE_PRICE_MONTHLY_ID=price_xxx
STRIPE_PRICE_6_MONTH_ID=price_xxx
STRIPE_PRICE_12_MONTH_ID=price_xxx
PUBLIC_BASE_URL=https://your-site-url.com

3) Webhook endpoint
-------------------
Add this endpoint in Stripe:

https://your-site-url.com/stripe/webhook

Events to send:
- checkout.session.completed
- customer.subscription.created
- customer.subscription.updated
- customer.subscription.deleted
- invoice.paid
- invoice.payment_succeeded
- invoice.payment_failed

4) Site admin
-------------
The Site Admin dashboard now shows:
- plan
- billing status
- trial end
- current period end
- last payment
- manual access status

The edit page lets you:
- change billing email
- change plan metadata
- change subscription status manually
- manually activate/bypass Stripe
- remove manual access
- view payment history

5) Access rules
---------------
Guild login is allowed when:
- manual_access = 1
- subscription_status = trialing
- subscription_status = active

Guild login is blocked when:
- disabled
- pending_billing
- canceled
- unpaid
- past_due
- incomplete

