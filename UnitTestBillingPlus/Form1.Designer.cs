namespace UnitTest
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
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
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.label1 = new System.Windows.Forms.Label();
            this.txtMaDViQLy = new System.Windows.Forms.TextBox();
            this.txtMaSoGCS = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.txtKy = new System.Windows.Forms.TextBox();
            this.label3 = new System.Windows.Forms.Label();
            this.txtThang = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.txtNam = new System.Windows.Forms.TextBox();
            this.label5 = new System.Windows.Forms.Label();
            this.txtCurrentLibID = new System.Windows.Forms.TextBox();
            this.label6 = new System.Windows.Forms.Label();
            this.txtWorkflowID = new System.Windows.Forms.TextBox();
            this.label7 = new System.Windows.Forms.Label();
            this.btnTinhHDon = new System.Windows.Forms.Button();
            this.btnHuyTinh = new System.Windows.Forms.Button();
            this.btnDChinh = new System.Windows.Forms.Button();
            this.label8 = new System.Windows.Forms.Label();
            this.txtPath = new System.Windows.Forms.TextBox();
            this.button1 = new System.Windows.Forms.Button();
            this.btnTinhHDonDC = new System.Windows.Forms.Button();
            this.btn_TinhHDonLe = new System.Windows.Forms.Button();
            this.btnTinhOnline = new System.Windows.Forms.Button();
            this.btnGetDataXML = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 9);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(55, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Mã đơn vị";
            // 
            // txtMaDViQLy
            // 
            this.txtMaDViQLy.Location = new System.Drawing.Point(92, 6);
            this.txtMaDViQLy.Name = "txtMaDViQLy";
            this.txtMaDViQLy.Size = new System.Drawing.Size(120, 20);
            this.txtMaDViQLy.TabIndex = 1;
            this.txtMaDViQLy.Text = "PD0600";
            // 
            // txtMaSoGCS
            // 
            this.txtMaSoGCS.Location = new System.Drawing.Point(92, 32);
            this.txtMaSoGCS.Name = "txtMaSoGCS";
            this.txtMaSoGCS.Size = new System.Drawing.Size(120, 20);
            this.txtMaSoGCS.TabIndex = 3;
            this.txtMaSoGCS.Text = "SO_THDON2;31/12/2022;16";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(12, 35);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(61, 13);
            this.label2.TabIndex = 2;
            this.label2.Text = "Mã sổ GCS";
            // 
            // txtKy
            // 
            this.txtKy.Location = new System.Drawing.Point(92, 58);
            this.txtKy.Name = "txtKy";
            this.txtKy.Size = new System.Drawing.Size(120, 20);
            this.txtKy.TabIndex = 5;
            this.txtKy.Text = "1";
            this.txtKy.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.txtKy_KeyPress);
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(12, 61);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(19, 13);
            this.label3.TabIndex = 4;
            this.label3.Text = "Kỳ";
            // 
            // txtThang
            // 
            this.txtThang.Location = new System.Drawing.Point(92, 84);
            this.txtThang.Name = "txtThang";
            this.txtThang.Size = new System.Drawing.Size(120, 20);
            this.txtThang.TabIndex = 7;
            this.txtThang.Text = "5";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(12, 87);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(38, 13);
            this.label4.TabIndex = 6;
            this.label4.Text = "Tháng";
            // 
            // txtNam
            // 
            this.txtNam.Location = new System.Drawing.Point(92, 110);
            this.txtNam.Name = "txtNam";
            this.txtNam.Size = new System.Drawing.Size(120, 20);
            this.txtNam.TabIndex = 9;
            this.txtNam.Text = "2023";
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(12, 113);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(29, 13);
            this.label5.TabIndex = 8;
            this.label5.Text = "Năm";
            // 
            // txtCurrentLibID
            // 
            this.txtCurrentLibID.Location = new System.Drawing.Point(92, 136);
            this.txtCurrentLibID.Name = "txtCurrentLibID";
            this.txtCurrentLibID.Size = new System.Drawing.Size(120, 20);
            this.txtCurrentLibID.TabIndex = 11;
            this.txtCurrentLibID.Text = "65";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(12, 139);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(72, 13);
            this.label6.TabIndex = 10;
            this.label6.Text = "Current Lib ID";
            // 
            // txtWorkflowID
            // 
            this.txtWorkflowID.Location = new System.Drawing.Point(92, 162);
            this.txtWorkflowID.Name = "txtWorkflowID";
            this.txtWorkflowID.Size = new System.Drawing.Size(120, 20);
            this.txtWorkflowID.TabIndex = 13;
            this.txtWorkflowID.Text = "7";
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Location = new System.Drawing.Point(12, 165);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(66, 13);
            this.label7.TabIndex = 12;
            this.label7.Text = "Workflow ID";
            // 
            // btnTinhHDon
            // 
            this.btnTinhHDon.Location = new System.Drawing.Point(92, 188);
            this.btnTinhHDon.Name = "btnTinhHDon";
            this.btnTinhHDon.Size = new System.Drawing.Size(120, 23);
            this.btnTinhHDon.TabIndex = 14;
            this.btnTinhHDon.Text = "Tính hóa đơn";
            this.btnTinhHDon.UseVisualStyleBackColor = true;
            this.btnTinhHDon.Click += new System.EventHandler(this.btnTinhHDon_Click);
            // 
            // btnHuyTinh
            // 
            this.btnHuyTinh.Location = new System.Drawing.Point(92, 298);
            this.btnHuyTinh.Name = "btnHuyTinh";
            this.btnHuyTinh.Size = new System.Drawing.Size(120, 23);
            this.btnHuyTinh.TabIndex = 15;
            this.btnHuyTinh.Text = "Hủy tính";
            this.btnHuyTinh.UseVisualStyleBackColor = true;
            this.btnHuyTinh.Click += new System.EventHandler(this.btnHuyTinh_Click);
            // 
            // btnDChinh
            // 
            this.btnDChinh.Location = new System.Drawing.Point(92, 360);
            this.btnDChinh.Name = "btnDChinh";
            this.btnDChinh.Size = new System.Drawing.Size(75, 23);
            this.btnDChinh.TabIndex = 16;
            this.btnDChinh.Text = "Điều chỉnh";
            this.btnDChinh.UseVisualStyleBackColor = true;
            this.btnDChinh.Click += new System.EventHandler(this.btnDChinh_Click);
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Location = new System.Drawing.Point(12, 330);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(40, 13);
            this.label8.TabIndex = 17;
            this.label8.Text = "File ds:";
            // 
            // txtPath
            // 
            this.txtPath.Location = new System.Drawing.Point(92, 330);
            this.txtPath.Name = "txtPath";
            this.txtPath.Size = new System.Drawing.Size(120, 20);
            this.txtPath.TabIndex = 18;
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(92, 217);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(120, 23);
            this.button1.TabIndex = 19;
            this.button1.Text = "Tính hóa đơn Plus";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.btnTinhHDonPlus_Click);
            // 
            // btnTinhHDonDC
            // 
            this.btnTinhHDonDC.Location = new System.Drawing.Point(173, 360);
            this.btnTinhHDonDC.Name = "btnTinhHDonDC";
            this.btnTinhHDonDC.Size = new System.Drawing.Size(114, 23);
            this.btnTinhHDonDC.TabIndex = 20;
            this.btnTinhHDonDC.Text = "Điều chỉnh Plus";
            this.btnTinhHDonDC.UseVisualStyleBackColor = true;
            this.btnTinhHDonDC.Click += new System.EventHandler(this.btnTinhHDonDC_Click);
            // 
            // btn_TinhHDonLe
            // 
            this.btn_TinhHDonLe.Location = new System.Drawing.Point(92, 246);
            this.btn_TinhHDonLe.Name = "btn_TinhHDonLe";
            this.btn_TinhHDonLe.Size = new System.Drawing.Size(120, 23);
            this.btn_TinhHDonLe.TabIndex = 21;
            this.btn_TinhHDonLe.Text = "Tính hóa đơn lẻ";
            this.btn_TinhHDonLe.UseVisualStyleBackColor = true;
            this.btn_TinhHDonLe.Click += new System.EventHandler(this.btn_TinhHDonLe_Click);
            // 
            // btnTinhOnline
            // 
            this.btnTinhOnline.Location = new System.Drawing.Point(92, 389);
            this.btnTinhOnline.Name = "btnTinhOnline";
            this.btnTinhOnline.Size = new System.Drawing.Size(75, 23);
            this.btnTinhOnline.TabIndex = 22;
            this.btnTinhOnline.Text = "Tính online";
            this.btnTinhOnline.UseVisualStyleBackColor = true;
            this.btnTinhOnline.Click += new System.EventHandler(this.btnTinhOnline_Click);
            // 
            // btnGetDataXML
            // 
            this.btnGetDataXML.Location = new System.Drawing.Point(217, 217);
            this.btnGetDataXML.Name = "btnGetDataXML";
            this.btnGetDataXML.Size = new System.Drawing.Size(120, 23);
            this.btnGetDataXML.TabIndex = 23;
            this.btnGetDataXML.Text = "Lấy dữ liệu XML";
            this.btnGetDataXML.UseVisualStyleBackColor = true;
            this.btnGetDataXML.Click += new System.EventHandler(this.btnGetDataXML_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(343, 430);
            this.Controls.Add(this.btnGetDataXML);
            this.Controls.Add(this.btnTinhOnline);
            this.Controls.Add(this.btn_TinhHDonLe);
            this.Controls.Add(this.btnTinhHDonDC);
            this.Controls.Add(this.button1);
            this.Controls.Add(this.txtPath);
            this.Controls.Add(this.label8);
            this.Controls.Add(this.btnDChinh);
            this.Controls.Add(this.btnHuyTinh);
            this.Controls.Add(this.btnTinhHDon);
            this.Controls.Add(this.txtWorkflowID);
            this.Controls.Add(this.label7);
            this.Controls.Add(this.txtCurrentLibID);
            this.Controls.Add(this.label6);
            this.Controls.Add(this.txtNam);
            this.Controls.Add(this.label5);
            this.Controls.Add(this.txtThang);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.txtKy);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.txtMaSoGCS);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.txtMaDViQLy);
            this.Controls.Add(this.label1);
            this.Name = "Form1";
            this.Text = "NTT4";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox txtMaDViQLy;
        private System.Windows.Forms.TextBox txtMaSoGCS;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox txtKy;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox txtThang;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.TextBox txtNam;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.TextBox txtCurrentLibID;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.TextBox txtWorkflowID;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.Button btnTinhHDon;
        private System.Windows.Forms.Button btnHuyTinh;
        private System.Windows.Forms.Button btnDChinh;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.TextBox txtPath;
        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.Button btnTinhHDonDC;
        private System.Windows.Forms.Button btn_TinhHDonLe;
        private System.Windows.Forms.Button btnTinhOnline;
        private System.Windows.Forms.Button btnGetDataXML;
    }
}

