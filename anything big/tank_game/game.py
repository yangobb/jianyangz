import pygame
import random

class Tank:
    def __init__(self, x, y, color, is_player=False):
        self.x = x
        self.y = y
        self.width = 40
        self.height = 40
        self.color = color
        self.speed = 3
        self.direction = "up"
        self.is_player = is_player
        self.lives = 3
        self.last_shot = pygame.time.get_ticks()
        self.shot_cooldown = 300
    
    def move(self, direction):
        self.direction = direction
        if direction == "up":
            self.y -= self.speed
        elif direction == "down":
            self.y += self.speed
        elif direction == "left":
            self.x -= self.speed
        elif direction == "right":
            self.x += self.speed
        
        self.x = max(0, min(800 - self.width, self.x))
        self.y = max(0, min(600 - self.height, self.y))
    
    def shoot(self):
        current_time = pygame.time.get_ticks()
        if current_time - self.last_shot > self.shot_cooldown:
            self.last_shot = current_time
            return Bullet(self.x + self.width // 2, self.y + self.height // 2, self.direction)
        return None
    
    def draw(self, screen):
        pygame.draw.rect(screen, self.color, (self.x, self.y, self.width, self.height))
        
        if self.direction == "up":
            pygame.draw.rect(screen, (0, 0, 0), (self.x + self.width // 3, self.y, self.width // 3, self.height // 2))
        elif self.direction == "down":
            pygame.draw.rect(screen, (0, 0, 0), (self.x + self.width // 3, self.y + self.height // 2, self.width // 3, self.height // 2))
        elif self.direction == "left":
            pygame.draw.rect(screen, (0, 0, 0), (self.x, self.y + self.height // 3, self.width // 2, self.height // 3))
        elif self.direction == "right":
            pygame.draw.rect(screen, (0, 0, 0), (self.x + self.width // 2, self.y + self.height // 3, self.width // 2, self.height // 3))

class Bullet:
    def __init__(self, x, y, direction):
        self.x = x
        self.y = y
        self.width = 4
        self.height = 4
        self.speed = 8
        self.direction = direction
        self.color = (0, 0, 0)
    
    def update(self):
        if self.direction == "up":
            self.y -= self.speed
        elif self.direction == "down":
            self.y += self.speed
        elif self.direction == "left":
            self.x -= self.speed
        elif self.direction == "right":
            self.x += self.speed
    
    def draw(self, screen):
        pygame.draw.rect(screen, self.color, (self.x, self.y, self.width, self.height))
    
    def is_out_of_bounds(self):
        return self.x < 0 or self.x > 800 or self.y < 0 or self.y > 600

class EnemyTank(Tank):
    def __init__(self, x, y):
        super().__init__(x, y, (255, 0, 0), is_player=False)
        self.speed = 1
        self.last_move_change = pygame.time.get_ticks()
        self.move_change_cooldown = 1000
        self.last_shot = pygame.time.get_ticks()
        self.shot_cooldown = 1000
    
    def update(self):
        current_time = pygame.time.get_ticks()
        
        if current_time - self.last_move_change > self.move_change_cooldown:
            self.last_move_change = current_time
            self.direction = random.choice(["up", "down", "left", "right"])
        
        self.move(self.direction)
    
    def shoot(self):
        current_time = pygame.time.get_ticks()
        if current_time - self.last_shot > self.shot_cooldown and random.random() < 0.1:
            self.last_shot = current_time
            return Bullet(self.x + self.width // 2, self.y + self.height // 2, self.direction)
        return None

class Wall:
    def __init__(self, x, y):
        self.x = x
        self.y = y
        self.width = 40
        self.height = 40
        self.color = (128, 128, 128)
    
    def draw(self, screen):
        pygame.draw.rect(screen, self.color, (self.x, self.y, self.width, self.height))
    
    def check_collision(self, obj):
        return (self.x < obj.x + obj.width and
                self.x + self.width > obj.x and
                self.y < obj.y + obj.height and
                self.y + self.height > obj.y)

class Game:
    def __init__(self, screen):
        self.screen = screen
        self.player = Tank(400, 500, (0, 255, 0), is_player=True)
        self.enemies = []
        self.bullets = []
        self.enemy_bullets = []
        self.walls = []
        self.score = 0
        self.game_over = False
        self.font = pygame.font.Font(None, 36)
        
        self.create_enemies()
        self.create_walls()
    
    def create_enemies(self):
        for i in range(3):
            for j in range(3):
                enemy = EnemyTank(100 + j * 150, 50 + i * 100)
                self.enemies.append(enemy)
    
    def create_walls(self):
        for i in range(5):
            wall = Wall(200 + i * 80, 200)
            self.walls.append(wall)
            wall = Wall(200 + i * 80, 250)
            self.walls.append(wall)
            wall = Wall(200 + i * 80, 300)
            self.walls.append(wall)
    
    def update(self):
        if self.game_over:
            return
        
        keys = pygame.key.get_pressed()
        if keys[pygame.K_UP]:
            self.player.move("up")
        if keys[pygame.K_DOWN]:
            self.player.move("down")
        if keys[pygame.K_LEFT]:
            self.player.move("left")
        if keys[pygame.K_RIGHT]:
            self.player.move("right")
        if keys[pygame.K_SPACE]:
            bullet = self.player.shoot()
            if bullet:
                self.bullets.append(bullet)
        
        for enemy in self.enemies:
            enemy.update()
            bullet = enemy.shoot()
            if bullet:
                self.enemy_bullets.append(bullet)
        
        for bullet in self.bullets:
            bullet.update()
            if bullet.is_out_of_bounds():
                self.bullets.remove(bullet)
        
        for bullet in self.enemy_bullets:
            bullet.update()
            if bullet.is_out_of_bounds():
                self.enemy_bullets.remove(bullet)
        
        self.check_collisions()
    
    def check_collisions(self):
        for bullet in self.bullets:
            for enemy in self.enemies:
                if (bullet.x < enemy.x + enemy.width and
                    bullet.x + bullet.width > enemy.x and
                    bullet.y < enemy.y + enemy.height and
                    bullet.y + bullet.height > enemy.y):
                    self.bullets.remove(bullet)
                    self.enemies.remove(enemy)
                    self.score += 100
                    break
            
            for wall in self.walls:
                if wall.check_collision(bullet):
                    self.bullets.remove(bullet)
                    self.walls.remove(wall)
                    break
        
        for bullet in self.enemy_bullets:
            if (bullet.x < self.player.x + self.player.width and
                bullet.x + bullet.width > self.player.x and
                bullet.y < self.player.y + self.player.height and
                bullet.y + bullet.height > self.player.y):
                self.enemy_bullets.remove(bullet)
                self.player.lives -= 1
                if self.player.lives <= 0:
                    self.game_over = True
                break
            
            for wall in self.walls:
                if wall.check_collision(bullet):
                    self.enemy_bullets.remove(bullet)
                    self.walls.remove(wall)
                    break
        
        if not self.enemies:
            self.game_over = True
    
    def draw(self):
        self.screen.fill((255, 255, 255))
        
        self.player.draw(self.screen)
        
        for enemy in self.enemies:
            enemy.draw(self.screen)
        
        for bullet in self.bullets:
            bullet.draw(self.screen)
        
        for bullet in self.enemy_bullets:
            bullet.draw(self.screen)
        
        for wall in self.walls:
            wall.draw(self.screen)
        
        score_text = self.font.render(f"分数: {self.score}", True, (0, 0, 0))
        self.screen.blit(score_text, (10, 10))
        
        lives_text = self.font.render(f"生命: {self.player.lives}", True, (0, 0, 0))
        self.screen.blit(lives_text, (10, 50))
        
        if self.game_over:
            game_over_text = self.font.render("游戏结束!", True, (255, 0, 0))
            self.screen.blit(game_over_text, (350, 250))
            if self.player.lives <= 0:
                result_text = self.font.render("你输了!", True, (255, 0, 0))
            else:
                result_text = self.font.render("你赢了!", True, (0, 255, 0))
            self.screen.blit(result_text, (350, 300))