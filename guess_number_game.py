import random

def guess_number_game():
    # 生成1到100之间的随机数
    secret_number = random.randint(1, 100)
    attempts = 0
    max_attempts = 7  # 设置最大尝试次数

    print("欢迎来到猜数字游戏！")
    print(f"我已经想好了一个1到100之间的数字，你有{max_attempts}次机会猜测。")

    while attempts < max_attempts:
        try:
            # 获取玩家输入
            guess = int(input("请输入你的猜测（1-100）: "))

            # 检查输入是否在有效范围内
            if guess < 1 or guess > 100:
                print("请输入1到100之间的数字！")
                continue

            attempts += 1

            # 比较猜测与目标数字
            if guess < secret_number:
                print("太小了！再试一次。")
            elif guess > secret_number:
                print("太大了！再试一次。")
            else:
                print(f"恭喜你猜对了！答案就是{secret_number}。")
                print(f"你用了{attempts}次尝试。")
                return

            # 提示剩余次数
            remaining = max_attempts - attempts
            if remaining > 0:
                print(f"你还有{remaining}次机会。")

        except ValueError:
            print("请输入有效的数字！")

    # 达到最大尝试次数
    print(f"很遗憾，你没有猜对。正确答案是{secret_number}。")
    print("游戏结束！")

if __name__ == "__main__":
    guess_number_game()

# 游戏说明：
# 1. 电脑随机生成1-100之间的数字
# 2. 玩家有7次机会猜测
# 3. 每次猜测后，电脑会提示"太大"、"太小"或"正确"
# 4. 如果在7次内猜对，玩家获胜；否则失败