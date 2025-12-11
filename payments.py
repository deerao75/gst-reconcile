import razorpay
import os
import time
from flask import Blueprint, render_template, request, session, redirect, url_for, flash
from datetime import date, timedelta

payment_bp = Blueprint('payments', __name__)

def get_razorpay_client():
    key_id = os.environ. get('RAZORPAY_KEY_ID')
    key_secret = os.environ.get('RAZORPAY_KEY_SECRET')
    if not key_id or not key_secret:
        raise ValueError("Razorpay keys are missing in .env")
    return razorpay. Client(auth=(key_id, key_secret))

def get_db_and_user():
    """Lazy import to avoid circular imports with RQ worker"""
    from main import db, User
    return db, User

@payment_bp. route('/create_order', methods=['POST'])
def create_order():
    db, User = get_db_and_user()

    if not session.get('logged_in'):
        return redirect(url_for('login'))

    plan = request.form.get('plan')
    amount = 0
    if plan == 'half-yearly':
        amount = 59000
    elif plan == 'annual':
        amount = 99900
    else:
        flash("Invalid plan selected", "danger")
        return redirect(url_for('subscribe'))

    client = get_razorpay_client()
    data = {
        "amount": amount,
        "currency": "INR",
        "receipt": f"rcpt_{int(time.time())}",
        "notes": {"email": session.get('email'), "plan": plan}
    }
    try:
        order = client.order.create(data=data)
        return render_template(
            'razorpay_checkout.html',
            order=order,
            key_id=os.environ.get('RAZORPAY_KEY_ID'),
            user_email=session.get('email'),
            user_contact=""
        )
    except Exception as e:
        flash(f"Error creating order: {str(e)}", "danger")
        return redirect(url_for('subscribe'))

@payment_bp.route('/payment_verification', methods=['POST'])
def payment_verification():
    db, User = get_db_and_user()

    razorpay_payment_id = request.form.get('razorpay_payment_id')
    razorpay_order_id = request.form.get('razorpay_order_id')
    razorpay_signature = request.form.get('razorpay_signature')
    client = get_razorpay_client()

    try:
        client.utility.verify_payment_signature({
            'razorpay_order_id': razorpay_order_id,
            'razorpay_payment_id': razorpay_payment_id,
            'razorpay_signature': razorpay_signature
        })

        order_details = client.order.fetch(razorpay_order_id)
        plan = order_details['notes']['plan']
        email = order_details['notes']['email']

        user = User.query.filter_by(email=email).first()
        if user:
            user.subscribed = True
            today = date.today()
            start_date = max(today, user.subscription_expiry_date) if user.subscription_expiry_date else today
            if plan == 'annual':
                user.subscription_expiry_date = start_date + timedelta(days=365)
            elif plan == 'half-yearly':
                user.subscription_expiry_date = start_date + timedelta(days=182)
            db.session.commit()
            flash('Payment Successful! Subscription activated. ', 'success')
            return redirect(url_for('index'))

    except Exception as e:
        flash(f"Payment verification failed: {str(e)}", "danger")
        return redirect(url_for('subscribe'))
