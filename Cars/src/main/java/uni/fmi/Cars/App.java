package uni.fmi.Cars;

import java.awt.BorderLayout;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.net.URI;

import javax.swing.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public class App extends JFrame {

    private JComboBox < String > resultComboBox;
    private JTextField brandTextField, horsePowerFromTextField, horsePowerToTextField, mpgTextField;
    private JButton searchButton;

    public static void main(String[] args) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                new App().createAndShowGUI();
            }
        });
    }

    private void createAndShowGUI() {
        JFrame frame = new JFrame("Car Search App");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        // GUI elements
        resultComboBox = new JComboBox < > (new String[] {
            "Average Consumption",
            "Car List"
        });
        resultComboBox.setSelectedItem("Average Consumption");

        brandTextField = new JTextFieldWithPlaceholder("Search by brand");

        horsePowerFromTextField = new JTextFieldWithPlaceholder("Horsepower from");

        horsePowerToTextField = new JTextFieldWithPlaceholder("Horsepower to");

        mpgTextField = new JTextFieldWithPlaceholder("Minimum MPG");

        searchButton = new JButton("Search");

        handleComboBoxChange();

        // Add an ActionListener to the ComboBox
        resultComboBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                handleComboBoxChange();
            }
        });

        searchButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                startHadoop();
            }
        });

        // Layout
        JPanel panel = new JPanel();
        panel.setLayout(new GridLayout(6, 1));
        panel.add(resultComboBox);
        panel.add(brandTextField);
        panel.add(horsePowerFromTextField);
        panel.add(horsePowerToTextField);
        panel.add(mpgTextField);
        panel.add(searchButton);

        // Set layout for the frame content pane
        frame.getContentPane().setLayout(new BorderLayout());
        frame.getContentPane().add(panel, BorderLayout.CENTER);

        // Set frame properties
        frame.setSize(300, 250);
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
    }

    private void handleComboBoxChange() {
        String selectedResult = (String) resultComboBox.getSelectedItem();

        // Enable or disable text fields based on the selected result
        boolean isAverageConsumption = "Average Consumption".equals(selectedResult);
        horsePowerFromTextField.setEnabled(!isAverageConsumption);
        horsePowerToTextField.setEnabled(!isAverageConsumption);
        mpgTextField.setEnabled(!isAverageConsumption);
    }

    private String filterType() {
        String selectedResult = (String) resultComboBox.getSelectedItem();
        boolean isAverageConsumption = "Average Consumption".equals(selectedResult);

        if (isAverageConsumption) {
            return "1";
        }

        return "0";

    }

    protected void startHadoop() {
        Configuration conf = new Configuration();
        conf.set("resultType", filterType()); // Set the result type ("average consumption" or "car list")
        conf.set("brand", brandTextField.getText()); // Set the brand filter
        conf.set("horsepowerFrom", horsePowerFromTextField.getText()); // Set the minimum horsepower filter
        conf.set("horsepowerTo", horsePowerToTextField.getText()); // Set the maximum horsepower filter
        conf.set("mpg", mpgTextField.getText()); // Set the minimum MPG filter


        JobConf job = new JobConf(conf, App.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapperClass(CarsMapper.class);
        job.setReducerClass(CarsReducer.class);

        Path input = new Path("hdfs://127.0.0.1:9000/input/cars.csv");
        Path output = new Path("hdfs://127.0.0.1:9000/cars");

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        try {
            FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);

            if (fs.exists(output))
                fs.delete(output, true);

            RunningJob task = JobClient.runJob(job);

            System.out.println("Is successfull: " + task.isSuccessful());

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}

class JTextFieldWithPlaceholder extends JTextField {
    private String placeholder;

    public JTextFieldWithPlaceholder(String placeholder) {
        this.placeholder = placeholder;
    }

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);

        if (getText().isEmpty()) {
            Graphics2D g2 = (Graphics2D) g.create();
            g2.setColor(getDisabledTextColor());
            g2.setFont(getFont().deriveFont(Font.ITALIC));
            g2.drawString(placeholder, getInsets().left, getHeight() - getInsets().bottom);
            g2.dispose();
        }
    }
}
