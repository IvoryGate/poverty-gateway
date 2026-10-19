"""FreeTier 错误文案分类：区分 IP abuse 与请求形状错误。"""

from app.core.freetier import is_abuse_message


def test_old_abuse_message():
    assert is_abuse_message(
        "Forbidden, OpenCode Free Model Gateway Detect Abuse Traffic")


def test_new_abuse_message():
    assert is_abuse_message(
        "OpenCode's free models aren't available from your current network "
        "because we've detected abusive traffic from this IP address. "
        "This isn't related to your account. If you're on a VPN or proxy, "
        "please disable it and try again.")


def test_shape_error_is_not_abuse():
    assert not is_abuse_message(
        "OpenCode's free tier can only be used from within OpenCode. "
        "Download it at https://opencode.ai and keep building.")


def test_unrelated_403_is_not_abuse():
    assert not is_abuse_message("Forbidden")
