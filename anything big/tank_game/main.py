import pygame
import sys
from game import Game

def main():
    pygame.init()
    screen = pygame.display.set_mode((800, 600))
    pygame.display.set_caption("坦克大战")
    clock = pygame.time.Clock()
    
    game = Game(screen)
    
    while True:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                pygame.quit()
                sys.exit()
        
        game.update()
        game.draw()
        
        pygame.display.flip()
        clock.tick(60)

if __name__ == "__main__":
    main()