﻿namespace FormParquet
{
	partial class Form1
	{
		/// <summary>
		///  Required designer variable.
		/// </summary>
		private System.ComponentModel.IContainer components = null;

		/// <summary>
		///  Clean up any resources being used.
		/// </summary>
		/// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
		protected override void Dispose(bool disposing)
		{
			if (disposing && (components != null))
			{
				components.Dispose();
			}
			base.Dispose(disposing);
		}

		#region Windows Form Designer generated code

		/// <summary>
		///  Required method for Designer support - do not modify
		///  the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			ek3_button = new Button();
			ek4_button = new Button();
			ek7_button = new Button();
			ek5_button = new Button();
			label1 = new Label();
			label2 = new Label();
			label3 = new Label();
			label4 = new Label();
			today = new DateTimePicker();
			tomarrow = new DateTimePicker();
			dateRange = new CheckBox();
			SuspendLayout();
			// 
			// ek3_button
			// 
			ek3_button.Location = new Point(12, 91);
			ek3_button.Name = "ek3_button";
			ek3_button.Size = new Size(116, 44);
			ek3_button.TabIndex = 1;
			ek3_button.Text = "EK3 (SFP)";
			ek3_button.UseVisualStyleBackColor = true;
			ek3_button.Click += ek3_button_Click;
			// 
			// ek4_button
			// 
			ek4_button.Location = new Point(12, 141);
			ek4_button.Name = "ek4_button";
			ek4_button.Size = new Size(116, 44);
			ek4_button.TabIndex = 2;
			ek4_button.Text = "EK4 (EPKBB)";
			ek4_button.UseVisualStyleBackColor = true;
			ek4_button.Click += ek4_button_Click;
			// 
			// ek7_button
			// 
			ek7_button.Location = new Point(12, 241);
			ek7_button.Name = "ek7_button";
			ek7_button.Size = new Size(116, 44);
			ek7_button.TabIndex = 3;
			ek7_button.Text = "EK7 (OKKIB)";
			ek7_button.UseVisualStyleBackColor = true;
			ek7_button.Click += ek7_button_Click;
			// 
			// ek5_button
			// 
			ek5_button.Location = new Point(12, 191);
			ek5_button.Name = "ek5_button";
			ek5_button.Size = new Size(116, 44);
			ek5_button.TabIndex = 4;
			ek5_button.Text = "EK5 (EPHPYCNI)";
			ek5_button.UseVisualStyleBackColor = true;
			ek5_button.Click += ek5_button_Click;
			// 
			// label1
			// 
			label1.AutoSize = true;
			label1.Font = new Font("Segoe UI", 10F, FontStyle.Bold, GraphicsUnit.Point);
			label1.Location = new Point(18, 58);
			label1.Name = "label1";
			label1.Size = new Size(308, 19);
			label1.TabIndex = 5;
			label1.Text = "Olusturmak istediginiz dosya tipine tiklayiniz";
			label1.Click += label1_Click;
			// 
			// label2
			// 
			label2.AutoSize = true;
			label2.Font = new Font("Calibri", 14.25F, FontStyle.Bold, GraphicsUnit.Point);
			label2.ForeColor = SystemColors.ActiveBorder;
			label2.Location = new Point(197, 362);
			label2.Name = "label2";
			label2.Size = new Size(200, 23);
			label2.TabIndex = 6;
			label2.Text = "Created by Ufuk Kürekci";
			// 
			// label3
			// 
			label3.AutoSize = true;
			label3.Location = new Point(185, 139);
			label3.Name = "label3";
			label3.Size = new Size(61, 15);
			label3.TabIndex = 9;
			label3.Text = "StartDate :";
			// 
			// label4
			// 
			label4.AutoSize = true;
			label4.Location = new Point(185, 191);
			label4.Name = "label4";
			label4.Size = new Size(60, 15);
			label4.TabIndex = 10;
			label4.Text = "EndDate : ";
			// 
			// today
			// 
			today.Location = new Point(260, 133);
			today.Name = "today";
			today.Size = new Size(188, 23);
			today.TabIndex = 12;
			// 
			// tomarrow
			// 
			tomarrow.Location = new Point(260, 185);
			tomarrow.Name = "tomarrow";
			tomarrow.Size = new Size(188, 23);
			tomarrow.TabIndex = 13;
			// 
			// dateRange
			// 
			dateRange.AutoSize = true;
			dateRange.Location = new Point(197, 105);
			dateRange.Name = "dateRange";
			dateRange.Size = new Size(88, 19);
			dateRange.TabIndex = 14;
			dateRange.Text = "Tarih Araligi";
			dateRange.UseVisualStyleBackColor = true;
			dateRange.CheckedChanged += dateRange_CheckedChanged;
			// 
			// Form1
			// 
			AutoScaleDimensions = new SizeF(7F, 15F);
			AutoScaleMode = AutoScaleMode.Font;
			ClientSize = new Size(480, 394);
			Controls.Add(dateRange);
			Controls.Add(tomarrow);
			Controls.Add(today);
			Controls.Add(label4);
			Controls.Add(label3);
			Controls.Add(label2);
			Controls.Add(label1);
			Controls.Add(ek5_button);
			Controls.Add(ek7_button);
			Controls.Add(ek4_button);
			Controls.Add(ek3_button);
			ForeColor = SystemColors.HotTrack;
			Name = "Form1";
			Text = "Form1";
			Load += Form1_Load;
			ResumeLayout(false);
			PerformLayout();
		}

		#endregion
		private Button ek3_button;
		private Button ek4_button;
		private Button ek7_button;
		private Button ek5_button;
		private Label label1;
		private Label label2;
		private Label label3;
		private Label label4;
		private DateTimePicker today;
		private DateTimePicker tomarrow;
		private CheckBox dateRange;
	}
}