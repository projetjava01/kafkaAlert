package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.Properties;
import java.time.Duration;

@SpringBootApplication
public class SimpleKafkaProducer implements CommandLineRunner {

    @Autowired
    private EmailService emailService;

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "alerts";
    private static final String GROUP_ID = "test-consumer-group";

    // NewsAPI key
    private static final String NEWS_API_KEY = "bd15358b810c4777afcb903da4c3c8a5"; // Remplace par ta clé NewsAPI
    private static final String NEWS_API_URL = "https://newsapi.org/v2/everything?q=%s&apiKey=%s";

    public static void main(String[] args) {
        SpringApplication.run(SimpleKafkaProducer.class, args);
    }

    @Override
    public void run(String... args) {
        Scanner scanner = new Scanner(System.in);

        // Demande des informations utilisateur
        System.out.print("Entrez votre email : ");
        String email = scanner.nextLine();

        System.out.print("Entrez le mot-clé pour les alertes : ");
        String keyword = scanner.nextLine();

        System.out.println("Choisissez la fréquence d'alerte : ");
        System.out.println("1 - À l'instant");
        System.out.println("2 - Dans deux jours");
        System.out.println("3 - Dans une semaine");

        int choice;
        do {
            System.out.print("Votre choix (1, 2 ou 3) : ");
            while (!scanner.hasNextInt()) {
                System.out.print("Veuillez entrer un nombre valide (1, 2 ou 3) : ");
                scanner.next();
            }
            choice = scanner.nextInt();
        } while (choice < 1 || choice > 3);
        scanner.nextLine(); // Nettoyage du buffer

        // Production immédiate du message
        produceMessage(keyword);

        // Planification de l'alerte
        scheduleAlert(email, keyword, choice);
    }

    // Méthode pour produire un message sur Kafka
    public void produceMessage(String message) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);
        producer.send(record);
        producer.close();
    }

    // Planification de l'alerte en fonction de la fréquence choisie
    public void scheduleAlert(String email, String keyword, int choice) {
        Timer timer = new Timer();
        long delay = switch (choice) {
            case 1 -> 0;                        // À l'instant
            case 2 -> 2 * 24 * 60 * 60 * 1000L; // Dans deux jours
            case 3 -> 7 * 24 * 60 * 60 * 1000L; // Dans une semaine
            default -> 0;
        };

        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                // Consommer les messages et envoyer les liens d'articles
                consumeMessagesAndSendLinks(email, keyword);
            }
        }, delay);
    }

    // Méthode pour consommer les messages et envoyer une alerte avec des liens d'articles
    public void consumeMessagesAndSendLinks(String email, String keyword) {
        // Recherche des articles en rapport avec le mot-clé via NewsAPI
        List<String> articleLinks = getArticlesFromNewsAPI(keyword);

        // Envoyer un email avec les liens des articles
        if (!articleLinks.isEmpty()) {
            emailService.sendEmailWithLinks(email, keyword, articleLinks);
        } else {
            emailService.sendEmail(email, keyword, "Aucun article trouvé pour ce mot-clé.");
        }
    }

    // Méthode pour récupérer les articles via NewsAPI
    public List<String> getArticlesFromNewsAPI(String keyword) {
        String url = String.format(NEWS_API_URL, keyword, NEWS_API_KEY);
        RestTemplate restTemplate = new RestTemplate();
        Map<String, Object> response = restTemplate.getForObject(url, Map.class);

        List<String> articleLinks = new ArrayList<>();
        if (response != null && response.containsKey("articles")) {
            List<Map<String, Object>> articles = (List<Map<String, Object>>) response.get("articles");
            for (Map<String, Object> article : articles) {
                String urlLink = (String) article.get("url");
                articleLinks.add(urlLink);
            }
        }
        return articleLinks;
    }

    // Configuration de JavaMailSender
    @Bean
    public JavaMailSender javaMailSender() {
        JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
        mailSender.setHost("smtp.gmail.com");
        mailSender.setPort(587);
        mailSender.setUsername("mariemmanuellakouadio30@gmail.com"); // Remplace par ton email
        mailSender.setPassword("zvnl vhyf wtgp bmul");   // Remplace par ton mot de passe ou mot de passe d'application

        Properties props = mailSender.getJavaMailProperties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.ssl.trust", "smtp.gmail.com");

        return mailSender;
    }
}

// Service pour l'envoi d'e-mail
@Service
class EmailService {

    @Autowired
    private JavaMailSender mailSender;

    public void sendEmail(String toEmail, String keyword, String messageContent) {
        try {
            SimpleMailMessage mailMessage = new SimpleMailMessage();
            mailMessage.setTo(toEmail);
            mailMessage.setSubject("Alerte Kafka - Mot-clé : " + keyword);
            mailMessage.setText(messageContent);

            mailSender.send(mailMessage);
            System.out.println("📧 Email envoyé à " + toEmail);
        } catch (Exception e) {
            System.err.println("Erreur lors de l'envoi de l'email : " + e.getMessage());
        }
    }

    // Envoi d'e-mail avec les liens des articles
    public void sendEmailWithLinks(String toEmail, String keyword, List<String> articleLinks) {
        StringBuilder emailContent = new StringBuilder("Voici les articles correspondant à votre alerte sur le mot-clé : " + keyword + "\n\n");
        for (String link : articleLinks) {
            emailContent.append(link).append("\n");
        }

        sendEmail(toEmail, keyword, emailContent.toString());
    }
}
